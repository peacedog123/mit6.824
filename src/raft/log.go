package raft

type Entry struct {
  Term int
  Index int
  Content []byte
}

type raftLog struct {
  entries         []Entry
  committed       int
  applied         int
}

func (t *raftLog) getCommitted() int {
  return t.committed
}

func (t *raftLog) getLastLog() *Entry {
  if (len(t.entries) == 0) {
    return nil
  } else {
    return &t.entries[len(t.entries) - 1]
  }
}

func (t *raftLog) getLastLogIndex() int {
  entry := t.getLastLog()
  if (entry == nil) {
    return 0
  } else {
    return entry.Index
  }
}

func (t *raftLog) getStartLogIndex() int {
  if (len(t.entries) == 0) {
    return 1
  } else {
    return t.entries[0].Index
  }
}

func (t *raftLog) getLastLogTerm() int {
  entry := t.getLastLog()
  if (entry == nil) {
    return 0
  } else {
    return entry.Term
  }
}

func (t *raftLog) getEntry(i int) Entry {
  return t.entries[i]
}

