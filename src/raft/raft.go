package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "bytes"
import "labgob"
import "log"
import "math/rand"
import "time"
import "sort"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// bg_timer tick in 100ms
const TickInMs int = 100

// election timer tick
const ElectionTickMax int = 6

// heartbeat timer
const HeartbeatTickMax int = 2

const NONE int = -1

type PeerInfo struct {
  nextIndex int
  matchIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
  cond      *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
  // recover from persisted state
  currentTerm       int                       // server's current term
  voteFor           int                       // current vote for
  log               *raftLog                  // log entry

  votes             map[int]bool              // vote result from peers
  leader            int                       // leader index in current term for redirect request

  peerInfo          map[int]*PeerInfo         // follower's state in the view of lead, reset every time

  role              Role                      // the server's Role

  election_timer    *bg_timer                 // bg timer for election and heartbeat
  election_tick     int                       // how many times election tick called
  election_tick_max int                       // election timeout (add random to prevent simultaneously RequestVote)

  heartbeat_timer   *bg_timer                 // bg timer for heartbeat if role==StateLeader
  heartbeat_tick    int                       // how many times heartbeat tick called

  applyCh           chan ApplyMsg             // the applyCh channel for committed entry
}

func init() {
  log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
}

func (rf *Raft) reset(term int) {
  rf.currentTerm = term
  rf.voteFor = NONE
  rf.leader = NONE

  rf.votes = make(map[int]bool)

  // 重新设置ElectionTimer和heartbeat
  rf.resetElectionTimer()
  rf.heartbeat_tick = 0

  // 重新设置PeerInfo
  rf.forEachPeer(func(id int, peer *PeerInfo) {
    peer.nextIndex = rf.log.getLastLogIndex() + 1
    if id == rf.me {
      peer.matchIndex = rf.log.getLastLogIndex()
    }
  })
}

func (rf *Raft) becomeCandidate() {
  rf.reset(rf.currentTerm + 1)
  rf.role = RoleCandidate
  rf.voteFor = rf.me
  rf.votes[rf.me] = true
  rf.leader = NONE
}

func (rf *Raft) becomeFollower(term int, leader int) {
  rf.reset(term)
  rf.role = RoleFollower
  rf.voteFor = NONE
  rf.leader = leader
}

func (rf *Raft) becomeLeader() {
  rf.reset(rf.currentTerm)
  rf.role = RoleLeader
  rf.leader = rf.me
}

func (rf *Raft) tickHeartbeat() {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  rf.heartbeat_tick++
  if rf.heartbeat_tick >= HeartbeatTickMax  {
    // 如果当前是RoleLeader, 则广播AppendEntries(可能没有Entry, 则退化为心跳消息)
    if rf.role == RoleLeader {
      rf.broadcastAppendEntries()
    }
    rf.heartbeat_tick = 0
  }
}

func (rf *Raft) tickElection() {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  rf.election_tick++
  if rf.election_tick >= rf.election_tick_max {
    if rf.role == RoleFollower || rf.role == RoleCandidate {
      log.Printf("WHO [%d] TAG [%s] - election tick up to max, start vote", rf.me, "TickElection")
      rf.becomeCandidate()
      rf.broadcastRequestVote()
    } else {
      rf.resetElectionTimer()
    }
  }
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
  rf.mu.Lock()
  defer rf.mu.Unlock()
  term = rf.currentTerm
  isleader = (rf.role == RoleLeader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
  w := new(bytes.Buffer)
  encoder := labgob.NewEncoder(w)
  encoder.Encode(rf.currentTerm)
  encoder.Encode(rf.voteFor)
  num := len(rf.log.unstable)
  encoder.Encode(len(rf.log.unstable))
  for i := 0; i < num; i++ {
    encoder.Encode(rf.log.unstable[i])
  }
  data := w.Bytes()
  rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
  r := bytes.NewBuffer(data)
  decoder := labgob.NewDecoder(r)
  if decoder.Decode(&rf.currentTerm) != nil {
    rf.currentTerm = 0
  }

  if decoder.Decode(&rf.voteFor) != nil {
    rf.voteFor = NONE
  }

  var num int
  if decoder.Decode(&num) != nil {
    return
  }

  for i := 0 ; i < num; i++ {
    entry := Entry {
    }

    if decoder.Decode(&entry) != nil {
      // TODO panic
    }
    rf.log.appendEntry(entry)
  }
}

func (rf *Raft) quorum() int { return len(rf.peers)/2 + 1 }

func (rf *Raft) poll(id int, vote bool) int {
  if vote {
    log.Printf("WHO [%d] TAG [%s] - received vote from [%x] at term [%d]", rf.me, "VoteSendRecv",  id, rf.currentTerm)
  } else {
    log.Printf("WHO [%d] TAG [%s] - rejected from [%x] at term [%d]", rf.me, "VoteSendRecv", id, rf.currentTerm)
  }

  if _, ok := rf.votes[id]; !ok {
    rf.votes[id] = vote
  }

  var granted int
  for _, vv := range rf.votes {
    if vv {
      granted++
    }
  }
  return granted
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  Term            int
  CandidateId     int
  LastLogTerm     int
  LastLogIndex    int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
  From        int     // the replier's id
  VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  log.Printf("WHO [%d] TAG [%s] - recv vote rquest From: [%d]\n", rf.me, "VoteRecv", request.CandidateId)

  reply.Term = rf.currentTerm
  reply.From = rf.me
  reply.VoteGranted = false

  if request.Term < rf.currentTerm {
    log.Printf("WHO [%d] TAG [%s] - recv vote request From: [%d], RequestTerm: [%d] smaller than CurrentTerm: [%d]",
              rf.me, "VoteRecv", request.CandidateId, request.Term, rf.currentTerm)
    return
  }
  if request.Term > rf.currentTerm {
    log.Printf("WHO [%d] TAG [%s] - recv vote request From: [%d] with higher term, RequestTerm: [%d], CurrentTerm: [%d] become follower",
                rf.me, "VoteRecv", request.CandidateId, request.Term, rf.currentTerm)
    rf.becomeFollower(request.Term, NONE)
  }

  // deal with term equal
  logOK := (request.LastLogTerm > rf.log.getLastLogTerm() ||
                (request.LastLogTerm == rf.log.getLastLogTerm() &&
                 request.LastLogIndex >= rf.log.getLastLogIndex()))
  if (logOK && (rf.voteFor == NONE || rf.voteFor == request.CandidateId)) {
    log.Printf("WHO [%d] TAG [%s] - grant vote to [%d] in term [%d]", rf.me, "VoteRecv", request.CandidateId, rf.currentTerm)
    reply.Term = rf.currentTerm
    reply.VoteGranted = true

    rf.voteFor = request.CandidateId
    rf.resetElectionTimer()
  }

  return
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote() {
  // retrieve the
  var request RequestVoteArgs
  request.Term = rf.currentTerm
  request.CandidateId = rf.me
  request.LastLogIndex = rf.log.getLastLogIndex()
  request.LastLogTerm = rf.log.getLastLogTerm()

  for i, _ := range rf.peers {
    if i == rf.me {
      continue
    }

    // 并行的发出rpc请求
    go func(arg RequestVoteArgs, index int) {
      log.Printf("WHO [%d] TAG [%s] - send request vote to [%d]", rf.me, "VoteSend", index)
      var reply RequestVoteReply
      ok := rf.sendRequestVote(index, &arg, &reply)
      // timeout or what ever
      log.Printf("WHO [%d] TAG [%s] - request vote to [%d], rpc status [%v]", rf.me, "VoteSendRecv", index, ok)
      if !ok {
        return
      }
      rf.mu.Lock()
      defer rf.mu.Unlock()
      // 判断请求发出之前的状态 - 有可能状态已经改变, 忽略这次的请求
      if (arg.Term != rf.currentTerm || rf.role != RoleCandidate) {
        log.Printf("WHO [%d] TAG [%s] - to [%d] rpc result discard. RequestTerm: [%d], CurrentTerm: [%d], Role: [%d]",
                   rf.me, "VoteSendRecv", index, arg.Term, rf.currentTerm, rf.role)
        return
      }

      // 如果reply的Term小于当前的Term, 则忽略
      if reply.Term < rf.currentTerm {
        log.Printf("WHO [%d] TAG [%s] - to [%d] get smaller term. ReplyTerm: [%d], CurrentTerm: [%d], Role: [%d]",
                   rf.me, "VoteSendRecv", index, reply.Term, rf.currentTerm, rf.role)
        return
      }
      // 如果reply的Term大于当前的Term, 则成为Follower
      if reply.Term > rf.currentTerm {
        log.Printf("WHO [%d] TAG [%s] - to [%d] get larger term. ReplyTerm: [%d], CurrentTerm: [%d], Role: [%d]",
                   rf.me, "VoteSendRecv", index, reply.Term, rf.currentTerm, rf.role)
        rf.becomeFollower(reply.Term, NONE)
        return
      }

      // Term相等的时候，判断投票
      if rf.poll(reply.From, reply.VoteGranted) >= rf.quorum() {
        log.Printf("WHO [%d] TAG [%s] - become leader in term [%d]", rf.me, "VoteSendRecv", rf.currentTerm)
        rf.becomeLeader()
        rf.broadcastAppendEntries()
      }
    }(request, i)
  }
}


func (rf *Raft) broadcastAppendEntries() {
  // no need lock!!!
  log.Printf("WHO [%d] TAG [%s] - start to broadcast", rf.me, "AppSend")

  // 对于每一个peer, 发送append消息
  for peerIndex, peer := range rf.peerInfo {
    // 忽略自己
    if peerIndex == rf.me {
      continue
    }
    var request AppendEntriesArgs
    request.Term = rf.currentTerm
    request.Leader = rf.leader
    request.LeaderCommit = rf.log.getCommitted()

    lastLogIndex := rf.log.getLastLogIndex();
    prevLogIndex := peer.nextIndex - 1;
    if !(prevLogIndex <= lastLogIndex) {
      log.Printf("Index: %d, PrevLogIndex: %d, LastLogIndex: %d", peerIndex, prevLogIndex, lastLogIndex)
      continue
    }

    if prevLogIndex >= rf.log.getStartLogIndex() {
      request.PrevLogTerm = rf.log.getEntry(prevLogIndex).Term
    } else {
      request.PrevLogTerm = 0
    }
    request.PrevLogIndex = prevLogIndex

    // entries
    numEntry := 0
    for i := peer.nextIndex; i <= lastLogIndex; i++ {
      entry := rf.log.getEntry(i)
      request.Entries = append(request.Entries, entry)
      numEntry++
    }
    request.LeaderCommit = min(rf.log.getCommitted(), prevLogIndex + numEntry)

    // send the append parallel
    go func(arg AppendEntriesArgs, toId int) {
      log.Printf("WHO [%d] TAG [%s] - prepare to send append message to [%d]", rf.me, "AppendSend", toId)
      var reply AppendEntriesReply
      success := rf.sendAppendEntries(toId, &arg, &reply)
      log.Printf("WHO [%d] TAG [%s] - append rpc to [%d] status is [%v].", rf.me, "AppendSendRecv", toId, success)
      if !success {
        return
      }

      // 锁住资源
      rf.mu.Lock()
      defer rf.mu.Unlock()

      // 如果当前的term和请求前不一样，或者role已经变化则忽略reply
      if rf.currentTerm != arg.Term || rf.role != RoleLeader {
        log.Printf("WHO [%d] TAG [%s] - state has has changed. RequestTerm: [%d], CurrentTerm: [%d], role: [%d]",
                    rf.me, "AppendSendRecv", arg.Term, rf.currentTerm, rf.role)
        return
      }

      // 如果reply的Term小于当前的term, 则忽略
      if reply.Term < rf.currentTerm {
        log.Printf("WHO [%d] TAG [%s] - to [%d] get smaller term. ReplyTerm: [%d], CurrentTerm: [%d], Role: [%d]",
                   rf.me, "AppendSendRecv", toId, reply.Term, rf.currentTerm, rf.role)
        return
      }

      // 如果reply的Term大于currentTerm，则转变为follower
      if reply.Term > rf.currentTerm {
        log.Printf("WHO [%d] TAG [%s] - to [%d] get larger term. ReplyTerm: [%d], CurrentTerm: [%d], Role: [%d]",
                   rf.me, "AppendSendRecv", toId, reply.Term, rf.currentTerm, rf.role)
        rf.becomeFollower(reply.Term, NONE)
        return
      }

      log.Printf("WHO [%d] TAG [%s] - to [%d] return: success: [%v], LastLogIndex: [%d]",
                  rf.me, "AppendSendRecv", toId, reply.Success, reply.LastLogIndex)

      // 如果返回成功，则更新matchIndex和nextIndex, 并尝试commit
      if reply.Success {
        rf.peerInfo[toId].matchIndex = prevLogIndex + len(arg.Entries)
        rf.peerInfo[toId].nextIndex = rf.peerInfo[toId].matchIndex + 1
        // 尝试增加commit index
        rf.advanceCommit()
      } else {
        // 缩小nextIndex下次再尝试
        //rf.peerInfo[toId].nextIndex = min(rf.peerInfo[toId].nextIndex - 1, reply.LastLogIndex + 1)
        rf.peerInfo[toId].nextIndex /= 2
        if rf.peerInfo[toId].nextIndex < 1 {
          rf.peerInfo[toId].nextIndex = 1
        }
      }
    }(request, peerIndex)
  }
}

type AppendEntriesArgs struct {
  Term            int
  Leader          int
  PrevLogIndex    int
  PrevLogTerm     int
  Entries         []Entry
  LeaderCommit    int
}

type AppendEntriesReply struct {
  Term            int
  Success         bool
  LastLogIndex    int
}

// RPC handler for AppendEntries request
func (rf *Raft) AppendEntries(request *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  log.Printf("WHO [%d] TAG [%s] - received appendentries request from [%d]", rf.me, "AppendRecv", request.Leader)

  reply.Term = rf.currentTerm
  reply.Success = false
  reply.LastLogIndex = rf.log.getLastLogIndex()

  // 如果请求的Term小于当前的Term, 忽略
  if (request.Term < rf.currentTerm) {
    log.Printf("WHO [%d] TAG [%s] - received request from [%d] term smaller. RequestTerm: [%d], CurrentTerm: [%d]",
                rf.me, "AppendRecv", request.Leader, request.Term, rf.currentTerm)

    return
  }

  // 如果请求的Term大于当前的Term, 则变更为Follower
  if (request.Term > rf.currentTerm) {
    log.Printf("WHO [%d] TAG [%s] - received request from [%d] term larger. RequestTerm: [%d], CurrentTerm: [%d]",
                rf.me, "AppendRecv", request.Leader, request.Term, rf.currentTerm)

    reply.Term = request.Term
  }

  rf.becomeFollower(request.Term, request.Leader)
  log.Printf("WHO [%d] TAG [%s] - leader: [%d], CurrentTerm: [%d] become follower",
                rf.me, "AppendRecv", request.Leader, rf.currentTerm)

  // 如果请求里面的PrevLogIndex大于lastLogIndex, 返回
  if (request.PrevLogIndex > rf.log.getLastLogIndex()) {
    log.Printf("WHO [%d] TAG [%s] - RequestPrevLogIndex: [%d], MyLastLogIndex: [%d], return.",
                rf.me, "AppendRecv", request.PrevLogIndex, rf.log.getLastLogIndex())
    return
  }

  // index 0比较特殊，因为index是从1开始
  if request.PrevLogIndex >= rf.log.getStartLogIndex() {
    myTerm := rf.log.getEntry(request.PrevLogIndex).Term
    if myTerm != request.PrevLogTerm {
      log.Printf("WHO [%d] TAG [%s] - log term mismatch. RequestPrevLogIndex: [%d], RequestPrevLogTerm: [%d], MyTerm: [%d]",
                rf.me, "AppendRecv", request.PrevLogIndex, request.PrevLogTerm, myTerm)
      return
    }
  }

  // If we got this far, we're accepting the request
  reply.Success = true

  // 处理Entry的删除和增加，这里要解决日志的truncation, 更新commitIndex等，同时触发applyCh的写
  //1. 找到index和term不匹配的点
  lastLogIndex := rf.log.getLastLogIndex()
  var match_i, truncate_index int
  match_i = -1
  for i:= 0; i < len(request.Entries); i++ {
    requestIndex := request.Entries[i].Index
    if (requestIndex != request.PrevLogIndex + i + 1) {
      panic("AppendEntriesArgs entries not continuous")
    }
    requestTerm := request.Entries[i].Term
    // 超出当前有的log, 需要append, append_i是开始append的点
    if requestIndex > lastLogIndex {
      break
    } else {
      entry := rf.log.getEntry(requestIndex)
      // 如果日志的index和term都匹配，继续走
      if requestIndex == entry.Index && requestTerm == entry.Term {
        match_i++
      } else {
        truncate_index = requestIndex
        break
      }
    }
  }
  // do truncation
  if truncate_index > 0 {
    rf.log.truncate(truncate_index)
  }

  // do append apppend the following entry
  for append_i := match_i + 1; append_i < len(request.Entries); append_i++ {
    rf.log.appendEntry(request.Entries[append_i])
  }

  // trigger commit if any
  newCommit := min(request.LeaderCommit, rf.log.getLastLogIndex())
  if (rf.log.getCommitted() < newCommit) {
    rf.log.setCommitted(newCommit)
    rf.cond.Signal()
  }

  reply.LastLogIndex = rf.log.getLastLogIndex()

  log.Printf("WHO [%d] TAG [%s] - CurrentTerm: [%d]. reply status. Success: [%v], LastLogIndex: [%d]",
              rf.me, "AppendRecv", rf.currentTerm, reply.Success, reply.LastLogIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
  return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  term = rf.currentTerm
  if rf.role != RoleLeader {
    isLeader = false
    log.Printf("WHO [%d] TAG [%s] - CurrentTerm: [%d]. Role: [%d]. Not leader, start return false",
              rf.me, "Start", rf.currentTerm, rf.role)
  } else {
    isLeader = true
    index = rf.log.getLastLogIndex() + 1
    rf.log.appendCommand(rf.currentTerm, index, command)
    rf.broadcastAppendEntries()
    log.Printf("WHO [%d] TAG [%s] - CurrentTerm: [%d]. Role: [%d]. Leader submit proposal, Index: [%d]",
              rf.me, "Start", rf.currentTerm, rf.role, index)
  }

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// reset the election timer
func (rf *Raft) resetElectionTimer() {
  rf.election_tick = 0
  rf.election_tick_max = ElectionTickMax + rand.Int() % 7
  log.Printf("WHO [%d] TAG [%s] - election_tick_max set to: [%d]", rf.me, "ResetElectionTimer", rf.election_tick_max)
}


func (rf *Raft) forEachPeer(f func(id int, pr *PeerInfo)) {
  for id, peer := range rf.peerInfo {
    f(id, peer)
  }
}

// wait for the committed cond then send to applyCh
func (rf *Raft) ApplyThread() {
  for {
    rf.mu.Lock()
    for !rf.log.HasEntryToApply() {
      rf.cond.Wait()
    }

    tempApp := make([]ApplyMsg, 0)
    indexSlice := make([]int, 0)

    // get new entry to apply, write to applyCh
    committed := rf.log.getCommitted()
    applied := rf.log.getApplied()
    for applied < committed {
      applied = applied + 1
      entry := rf.log.getEntry(applied)
      // get the
      msg := ApplyMsg {
        CommandValid: true,
        Command: entry.Content,
        CommandIndex: applied,
      }
      tempApp = append(tempApp, msg)
      indexSlice = append(indexSlice, applied)
    }
    rf.log.setApplied(applied)
    rf.mu.Unlock()

    log.Printf("WHO [%d] TAG [%s] - CurrentTerm: [%d]. AppliedIndex: [%v]",
                rf.me, "ApplyThread", rf.currentTerm, indexSlice)

    // may block, so write it without mutex
    for _, msg := range tempApp {
      rf.applyCh <- msg
    }
  }
}

// 当unstable复制到大多数机器后，需要增加committed
func (rf *Raft) advanceCommit() {
  quorum := rf.quorumMatchIndex()
  if (quorum > rf.log.getCommitted()) {
    log.Printf("WHO [%d] TAG [%s] - CurrentTerm: [%d], will commit from [%d] to [%d]",
                   rf.me, "AdvanceCommit", rf.log.getCommitted(), quorum)
    rf.log.commit(quorum, rf.currentTerm, rf.voteFor)
    rf.cond.Signal()
  }
}

// 判断当前quorum的MatchIndex
func (rf *Raft) quorumMatchIndex() int {
  buf := make(intSlice, len(rf.peerInfo))
  for index, peer := range rf.peerInfo {
    // 这里不保存peer中自己的matchIndex, 直接从log中读取
    if index == rf.me {
      buf[index] = rf.log.getLastLogIndex()
    } else {
      buf[index] = peer.matchIndex
    }
  }
  log.Printf("WHO [%d] TAG [%s] - CurrentTerm: [%d]. quorum match index: [%v]",
              rf.me, "QuorumMatchIndex", rf.currentTerm, buf)
  sort.Sort(buf)
  return buf[len(buf)/2]
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
  rf.cond = sync.NewCond(&rf.mu)
  rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

  // the random seed
  rand.Seed(time.Now().UnixNano())

  rf.currentTerm = 0
  rf.voteFor = NONE
  rf.leader = NONE
  rf.becomeFollower(rf.currentTerm, NONE)

  // initialize peer info
  rf.peerInfo = make(map[int]*PeerInfo)
  for index, _ := range rf.peers {
    rf.peerInfo[index] = &PeerInfo {
      nextIndex: 1,
    }
  }

  // initialize the election timer
  rf.election_timer = &bg_timer {
    should_stop: false,
    tick: TickInMs,
    done: make(chan bool),
    cb: rf.tickElection,
  }

  // initialize the heartbeat timer
  rf.heartbeat_timer = &bg_timer {
    should_stop: false,
    tick: TickInMs,
    done: make(chan bool),
    cb: rf.tickHeartbeat,
  }

  // initialize the log
  rf.log = &raftLog {
    committed: 0,
    applied: 0,
  }

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

  // start the bg_timer
  rf.election_timer.run()
  rf.heartbeat_timer.run()

  // the apply thread
  go rf.ApplyThread()

	return rf
}
