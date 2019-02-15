package raft

type Role int

const (
  RoleFollower Role = iota
  RoleCandidate
  RoleLeader
)

// intSlice implements sort interface
type intSlice []int

func (p intSlice) Len() int           { return len(p) }
func (p intSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p intSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
