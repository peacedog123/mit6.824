package raft

import (
  "time"
)

type bg_timer struct {
  stop bool
  tick int
  done chan bool
  cb func()
}

func(t *bg_timer) run() {
  go func() {
    for !t.stop {
      time.Sleep((time.Duration(t.tick)) * time.Millisecond)
      t.cb()
    }
    t.done <- true
  }()
}
