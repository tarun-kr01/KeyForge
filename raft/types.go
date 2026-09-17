package raft

import "errors"

// ID uniquely identifies a server in a cluster.
type ID string

var (
	ErrNotLeader      = errors.New("raft: node is not leader")
	ErrStopped        = errors.New("raft: node is stopped")
	ErrTimeout        = errors.New("raft: operation timed out")
	ErrNoQuorum       = errors.New("raft: quorum unavailable")
	ErrMemberExists   = errors.New("raft: member already exists")
	ErrMemberNotFound = errors.New("raft: member not found")
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is a replicated state-machine command. Index is assigned by the
// leader and is retained in snapshots for diagnostics.
type LogEntry struct {
	Index uint64
	Term  uint64
	Type  string
	Data  []byte
}

const (
	commandPut    = "put"
	commandDelete = "delete"
	commandNoop   = "noop"
	commandConfig = "config"
)

type OperationResult struct {
	Value   string
	Found   bool
	Applied uint64
}

type Status struct {
	ID          ID
	Role        Role
	Term        uint64
	Leader      ID
	CommitIndex uint64
	LastApplied uint64
	Snapshot    uint64
	Members     []ID
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  ID
	LastLogIndex uint64
	LastLogTerm  uint64
}
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     ID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictTerm  uint64
	ConflictIndex uint64
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          ID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}
type InstallSnapshotReply struct{ Term uint64 }
