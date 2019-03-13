package raftkv

import (
  "fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
  Type            string        // "get" or "put" or "append"
  Key             string
  Value           string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
  kvMap   map[string]string     // holder for the result map
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
  op := Op {
    Type: "Get",
    Key: args.Key,
  }

  _, _, isLeader := kv.rf.Start(op)
  if !isLeader {
    reply.WrongLeader = true
    reply.Err = Err(fmt.Sprintf("%v not leader", kv.me))
  } else {
    select {
      case <-kv.applyCh:
        kv.mu.Lock()
        value, ok := kv.kvMap[op.Key]
        kv.mu.Unlock()
        reply.WrongLeader = false
        if !ok {
          reply.Value = ""
        } else {
          reply.Value = value
        }
    }
  }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
  op := Op {
    Key: args.Key,
    Value: args.Value,
    Type: args.Op,
  }

  _, _, isLeader := kv.rf.Start(op)
  if !isLeader {
    reply.WrongLeader = true
    reply.Err = Err(fmt.Sprintf("%v not leader", kv.me))
  } else {
    select {
      case <-kv.applyCh:
      kv.mu.Lock()
      if op.Type == "Append" {
        kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
      } else {
        kv.kvMap[op.Key] = op.Value
      }
      kv.mu.Unlock()
    }
  }
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
  kv.kvMap = make(map[string]string)

	// You may need initialization code here.

	return kv
}
