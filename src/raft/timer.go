package raft

import (
  "time"
)

type bg_timer struct {
  should_stop bool
  tick int
  done chan bool
  cb func()
}

func(t *bg_timer) stop() {
  t.should_stop = true
  <- t.done
}

func(t *bg_timer) run() {
  go func() {
    for !t.should_stop {
      time.Sleep((time.Duration(t.tick)) * time.Millisecond)
      t.cb()
    }
    t.done <- true
  }()
}
