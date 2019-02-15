package raft

type Entry struct {
  Term          int
  Index         int
  Content       interface{}
}

type raftLog struct {
  unstable        []Entry
  committed       int
  applied         int
}

func (r *raftLog) getCommitted() int {
  return r.committed
}

func (r *raftLog) setCommitted(value int) {
  r.committed = value
}

func (r *raftLog) getApplied() int {
  return r.applied
}

func (r *raftLog) setApplied(value int) {
  r.applied = value
}

func (r *raftLog) getLastLogIndex() int {
  if len(r.unstable) == 0 {
    return 0
  } else {
    return r.unstable[len(r.unstable) - 1].Index
  }
}

func (r *raftLog) getStartLogIndex() int {
  if len(r.unstable) == 0 {
    return 1
  } else {
    return r.unstable[0].Index
  }
}

func (r *raftLog) getLastLogTerm() int {
  if (len(r.unstable) == 0) {
    return 0
  } else {
    return r.unstable[len(r.unstable) - 1].Term
  }
}

// 从Index到Pos的映射需要-1
func (r *raftLog) getEntry(index int) Entry {
  if index >= r.getStartLogIndex() && index <= r.getLastLogIndex() {
    return r.unstable[index - 1]
  } else {
    panic("Invalid index")
  }
}

func (r *raftLog) appendCommand(term int, index int, command interface{}) {
  entry := Entry {
    Index: index,
    Term: term,
    Content: command,
  }

  r.unstable = append(r.unstable, entry)
}

func (r *raftLog) appendEntry(entry Entry) {
  r.unstable = append(r.unstable, entry)
}

func (r *raftLog) truncate(index int) {
  if len(r.unstable) == 0 {
    return
  }
  var pos int
  for _, entry := range r.unstable {
    if entry.Index < index {
      pos++
    } else {
      break
    }
  }
  if pos == 0 {
    r.unstable = make([]Entry, 0)
  } else {
    r.unstable = r.unstable[0:pos]
  }
}

func (r *raftLog) HasEntryToApply() bool {
  return r.committed > r.applied
}

// commit the index
// 把unstable中的数据移入Storage并落盘
func (r *raftLog) commit(commitTo int, currentTerm int, voteFor int) {
  if r.committed >= commitTo {
    return
  }
  r.setCommitted(commitTo)
}
