package raft

import (
	"encoding/json"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

type Options struct {
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	OperationTimeout  time.Duration
	Seed              int64
	Storage           *MemoryStorage
}

func (o *Options) setDefaults() {
	if o.ElectionTimeout <= 0 {
		o.ElectionTimeout = 250 * time.Millisecond
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 50 * time.Millisecond
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = 2 * time.Second
	}
}

type command struct {
	Key, Value string
	Members    []ID
}
type snapshotState struct {
	KV      map[string]string
	Members []ID
}

type Node struct {
	mu                            sync.Mutex
	id                            ID
	transport                     Transport
	storage                       *MemoryStorage
	opts                          Options
	role                          Role
	currentTerm                   uint64
	votedFor                      ID
	leader                        ID
	members                       map[ID]bool
	log                           []LogEntry
	snapshotIndex, snapshotTerm   uint64
	kv                            map[string]string
	commitIndex, lastApplied      uint64
	nextIndex, matchIndex         map[ID]uint64
	waiters                       map[uint64]chan OperationResult
	electionDeadline, heartbeatAt time.Time
	rng                           *rand.Rand
	stop                          chan struct{}
	done                          chan struct{}
	started                       bool
}

func NewNode(id ID, members []ID, transport Transport, opts Options) *Node {
	opts.setDefaults()
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(id))
	seed := opts.Seed + int64(hash.Sum64())
	n := &Node{id: id, transport: transport, opts: opts, storage: opts.Storage, role: Follower, members: map[ID]bool{}, kv: map[string]string{}, nextIndex: map[ID]uint64{}, matchIndex: map[ID]uint64{}, waiters: map[uint64]chan OperationResult{}, rng: rand.New(rand.NewSource(seed))}
	if n.storage == nil {
		n.storage = NewMemoryStorage()
	}
	n.members[id] = true
	for _, m := range members {
		n.members[m] = true
	}
	n.currentTerm, n.votedFor = n.storage.State()
	n.log = n.storage.Log()
	s := n.storage.GetSnapshot()
	n.snapshotIndex, n.snapshotTerm = s.LastIncludedIndex, s.LastIncludedTerm
	n.restoreSnapshotLocked(s.Data)
	n.lastApplied, n.commitIndex = n.snapshotIndex, n.snapshotIndex
	n.resetElectionLocked()
	if transport != nil {
		transport.Register(id, n)
	}
	return n
}

func (n *Node) ID() ID { return n.id }

// RequestVote, AppendEntries, and InstallSnapshot expose the Raft RPC surface
// for network transports. MemoryTransport calls the unexported handlers.
func (n *Node) RequestVote(a RequestVoteArgs) RequestVoteReply {
	return n.handleRequestVote(a)
}
func (n *Node) AppendEntries(a AppendEntriesArgs) AppendEntriesReply {
	return n.handleAppendEntries(a)
}
func (n *Node) InstallSnapshot(a InstallSnapshotArgs) InstallSnapshotReply {
	return n.handleInstallSnapshot(a)
}

func (n *Node) Start() {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return
	}
	n.stop, n.done = make(chan struct{}), make(chan struct{})
	n.started = true
	if n.transport != nil {
		n.transport.Register(n.id, n)
	}
	n.resetElectionLocked()
	n.mu.Unlock()
	go n.run()
}
func (n *Node) Stop() {
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return
	}
	n.started = false
	stop, done := n.stop, n.done
	close(stop)
	n.mu.Unlock()
	<-done
	if n.transport != nil {
		n.transport.Unregister(n.id)
	}
}
func (n *Node) run() {
	defer close(n.done)
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.tick()
		case <-n.stop:
			return
		}
	}
}
func (n *Node) resetElectionLocked() {
	spread := n.opts.ElectionTimeout / 2
	extra := time.Duration(0)
	if spread > 0 {
		extra = time.Duration(n.rng.Int63n(int64(spread)))
	}
	n.electionDeadline = time.Now().Add(n.opts.ElectionTimeout + extra)
}
func (n *Node) tick() {
	n.mu.Lock()
	now := time.Now()
	if n.role == Leader {
		if now.Before(n.heartbeatAt) {
			n.mu.Unlock()
			return
		}
		n.heartbeatAt = now.Add(n.opts.HeartbeatInterval)
		peers := n.peerIDsLocked()
		n.mu.Unlock()
		for _, p := range peers {
			go n.replicate(p)
		}
		return
	}
	if now.Before(n.electionDeadline) {
		n.applyLocked()
		n.mu.Unlock()
		return
	}
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.leader = ""
	n.resetElectionLocked()
	n.storage.SetState(n.currentTerm, n.votedFor)
	term, idx, lastTerm := n.currentTerm, n.lastIndexLocked(), n.termAtLocked(n.lastIndexLocked())
	peers := n.peerIDsLocked()
	quorum := n.quorumLocked()
	n.mu.Unlock()
	votes := 1
	if votes >= quorum {
		n.mu.Lock()
		if n.role == Candidate && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}
	for _, p := range peers {
		go func(peer ID) {
			if n.transport == nil {
				return
			}
			r, err := n.transport.SendRequestVote(n.id, peer, RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: idx, LastLogTerm: lastTerm})
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if r.Term > n.currentTerm {
				n.becomeFollowerLocked(r.Term, "")
				return
			}
			if n.role != Candidate || n.currentTerm != term || !r.VoteGranted {
				return
			}
			votes++
			if votes >= n.quorumLocked() {
				n.becomeLeaderLocked()
			}
		}(p)
	}
}

func (n *Node) peerIDsLocked() []ID {
	out := make([]ID, 0, len(n.members))
	for p := range n.members {
		if p != n.id {
			out = append(out, p)
		}
	}
	return out
}
func (n *Node) quorumLocked() int       { return len(n.members)/2 + 1 }
func (n *Node) lastIndexLocked() uint64 { return n.snapshotIndex + uint64(len(n.log)) }
func (n *Node) termAtLocked(index uint64) uint64 {
	if index == n.snapshotIndex {
		return n.snapshotTerm
	}
	if index < n.snapshotIndex || index > n.lastIndexLocked() {
		return 0
	}
	return n.log[index-n.snapshotIndex-1].Term
}
func (n *Node) becomeFollowerLocked(term uint64, leader ID) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role, n.leader = Follower, leader
	n.storage.SetState(n.currentTerm, n.votedFor)
	n.resetElectionLocked()
}
func (n *Node) becomeLeaderLocked() {
	n.role, n.leader = Leader, n.id
	next := n.lastIndexLocked() + 1
	for p := range n.members {
		if p != n.id {
			n.nextIndex[p], n.matchIndex[p] = next, 0
		}
	}
	n.heartbeatAt = time.Time{}
}

func (n *Node) handleRequestVote(a RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if a.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm}
	}
	if a.Term > n.currentTerm {
		n.becomeFollowerLocked(a.Term, "")
	}
	upToDate := a.LastLogTerm > n.termAtLocked(n.lastIndexLocked()) || (a.LastLogTerm == n.termAtLocked(n.lastIndexLocked()) && a.LastLogIndex >= n.lastIndexLocked())
	granted := (n.votedFor == "" || n.votedFor == a.CandidateID) && upToDate
	if granted {
		n.votedFor = a.CandidateID
		n.storage.SetState(n.currentTerm, n.votedFor)
		n.resetElectionLocked()
	}
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: granted}
}
func (n *Node) handleAppendEntries(a AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if a.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm}
	}
	if a.Term > n.currentTerm || n.role != Follower {
		n.becomeFollowerLocked(a.Term, a.LeaderID)
	} else {
		n.leader = a.LeaderID
		n.resetElectionLocked()
	}
	if a.PrevLogIndex < n.snapshotIndex {
		return AppendEntriesReply{Term: n.currentTerm, ConflictIndex: n.snapshotIndex + 1}
	}
	if n.termAtLocked(a.PrevLogIndex) != a.PrevLogTerm {
		ci := n.lastIndexLocked() + 1
		if a.PrevLogIndex <= n.lastIndexLocked() {
			ci = a.PrevLogIndex
			ct := n.termAtLocked(a.PrevLogIndex)
			for ci > n.snapshotIndex && n.termAtLocked(ci-1) == ct {
				ci--
			}
			return AppendEntriesReply{Term: n.currentTerm, ConflictIndex: ci, ConflictTerm: ct}
		}
		return AppendEntriesReply{Term: n.currentTerm, ConflictIndex: ci}
	}
	for _, e := range a.Entries {
		if e.Index <= n.snapshotIndex {
			continue
		}
		if e.Index <= n.lastIndexLocked() && n.termAtLocked(e.Index) != e.Term {
			n.log = n.log[:e.Index-n.snapshotIndex-1]
		}
		if e.Index > n.lastIndexLocked() {
			n.log = append(n.log, e)
		}
	}
	n.storage.SetLog(n.log)
	if a.LeaderCommit > n.commitIndex {
		n.commitIndex = min(a.LeaderCommit, n.lastIndexLocked())
	}
	n.applyLocked()
	return AppendEntriesReply{Term: n.currentTerm, Success: true, MatchIndex: n.lastIndexLocked()}
}
func (n *Node) handleInstallSnapshot(a InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if a.Term < n.currentTerm {
		return InstallSnapshotReply{Term: n.currentTerm}
	}
	n.becomeFollowerLocked(a.Term, a.LeaderID)
	if a.LastIncludedIndex <= n.snapshotIndex {
		return InstallSnapshotReply{Term: n.currentTerm}
	}
	n.restoreSnapshotLocked(a.Data)
	n.snapshotIndex, n.snapshotTerm = a.LastIncludedIndex, a.LastIncludedTerm
	n.log = nil
	n.commitIndex, n.lastApplied = n.snapshotIndex, n.snapshotIndex
	n.storage.SetSnapshot(Snapshot{a.LastIncludedIndex, a.LastIncludedTerm, a.Data})
	n.storage.SetLog(nil)
	return InstallSnapshotReply{Term: n.currentTerm}
}

func (n *Node) restoreSnapshotLocked(data []byte) {
	if len(data) == 0 {
		return
	}
	var state snapshotState
	if err := json.Unmarshal(data, &state); err == nil && state.KV != nil {
		n.kv = state.KV
		if len(state.Members) > 0 {
			n.members = make(map[ID]bool, len(state.Members))
			for _, id := range state.Members {
				n.members[id] = true
			}
		}
		return
	}
	_ = json.Unmarshal(data, &n.kv)
}

func (n *Node) replicate(peer ID) {
	if n.transport == nil {
		return
	}
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	if next == 0 {
		next = n.lastIndexLocked() + 1
		n.nextIndex[peer] = next
	}
	if next <= n.snapshotIndex {
		s := n.storage.GetSnapshot()
		a := InstallSnapshotArgs{Term: n.currentTerm, LeaderID: n.id, LastIncludedIndex: s.LastIncludedIndex, LastIncludedTerm: s.LastIncludedTerm, Data: s.Data}
		n.mu.Unlock()
		r, err := n.transport.SendInstallSnapshot(n.id, peer, a)
		if err != nil {
			return
		}
		n.mu.Lock()
		if r.Term > n.currentTerm {
			n.becomeFollowerLocked(r.Term, "")
			n.mu.Unlock()
			return
		}
		n.nextIndex[peer] = s.LastIncludedIndex + 1
		n.matchIndex[peer] = s.LastIncludedIndex
		n.mu.Unlock()
		return
	}
	prev := next - 1
	offset := next - n.snapshotIndex - 1
	if offset > uint64(len(n.log)) {
		offset = uint64(len(n.log))
	}
	entries := cloneEntries(n.log[offset:])
	a := AppendEntriesArgs{Term: n.currentTerm, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: n.termAtLocked(prev), Entries: entries, LeaderCommit: n.commitIndex}
	n.mu.Unlock()
	r, err := n.transport.SendAppendEntries(n.id, peer, a)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.Term > n.currentTerm {
		n.becomeFollowerLocked(r.Term, "")
		return
	}
	if n.role != Leader || a.Term != n.currentTerm {
		return
	}
	if r.Success {
		n.matchIndex[peer], n.nextIndex[peer] = r.MatchIndex, r.MatchIndex+1
		n.advanceCommitLocked()
	} else if n.nextIndex[peer] > 1 {
		if r.ConflictIndex > 0 {
			n.nextIndex[peer] = r.ConflictIndex
		} else {
			n.nextIndex[peer]--
		}
	}
}
func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndexLocked(); idx > n.commitIndex; idx-- {
		if n.termAtLocked(idx) != n.currentTerm {
			continue
		}
		count := 1
		for p := range n.members {
			if p != n.id && n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorumLocked() {
			n.commitIndex = idx
			n.applyLocked()
			return
		}
	}
}
func (n *Node) applyLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.lastApplied <= n.snapshotIndex {
			continue
		}
		e := n.log[n.lastApplied-n.snapshotIndex-1]
		var c command
		_ = json.Unmarshal(e.Data, &c)
		result := OperationResult{Applied: e.Index}
		switch e.Type {
		case commandPut:
			n.kv[c.Key] = c.Value
			result.Value, result.Found = c.Value, true
		case commandDelete:
			result.Value, result.Found = n.kv[c.Key]
			delete(n.kv, c.Key)
		case commandConfig:
			n.members = make(map[ID]bool, len(c.Members))
			for _, m := range c.Members {
				n.members[m] = true
			}
			if n.role == Leader {
				next := n.lastIndexLocked() + 1
				for member := range n.members {
					if member != n.id {
						if _, ok := n.nextIndex[member]; !ok {
							n.nextIndex[member] = next
						}
					}
				}
			}
		}
		if w := n.waiters[e.Index]; w != nil {
			delete(n.waiters, e.Index)
			w <- result
			close(w)
		}
	}
}

func (n *Node) propose(typ string, c command) (OperationResult, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return OperationResult{}, err
	}
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return OperationResult{}, ErrStopped
	}
	if n.role != Leader {
		n.mu.Unlock()
		return OperationResult{}, ErrNotLeader
	}
	idx := n.lastIndexLocked() + 1
	n.log = append(n.log, LogEntry{Index: idx, Term: n.currentTerm, Type: typ, Data: data})
	n.storage.SetLog(n.log)
	ch := make(chan OperationResult, 1)
	n.waiters[idx] = ch
	peers := n.peerIDsLocked()
	if n.quorumLocked() == 1 {
		n.commitIndex = idx
		n.applyLocked()
	}
	n.mu.Unlock()
	for _, p := range peers {
		go n.replicate(p)
	}
	select {
	case r := <-ch:
		return r, nil
	case <-time.After(n.opts.OperationTimeout):
		n.mu.Lock()
		delete(n.waiters, idx)
		n.mu.Unlock()
		return OperationResult{}, ErrTimeout
	}
}

func (n *Node) Put(key, value string) error {
	_, err := n.propose(commandPut, command{Key: key, Value: value})
	return err
}
func (n *Node) Delete(key string) error {
	_, err := n.propose(commandDelete, command{Key: key})
	return err
}
func (n *Node) Get(key string) (string, bool, error) {
	if _, err := n.propose(commandNoop, command{}); err != nil {
		return "", false, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.kv[key]
	return v, ok, nil
}
func (n *Node) AddMember(id ID) error {
	if id == "" {
		return ErrMemberNotFound
	}
	n.mu.Lock()
	exists := n.members[id]
	n.mu.Unlock()
	if exists {
		return ErrMemberExists
	}
	members := n.Members()
	members = append(members, id)
	_, err := n.propose(commandConfig, command{Members: members})
	return err
}
func (n *Node) RemoveMember(id ID) error {
	if id == n.id {
		return ErrMemberNotFound
	}
	n.mu.Lock()
	if !n.members[id] {
		n.mu.Unlock()
		return ErrMemberNotFound
	}
	members := make([]ID, 0, len(n.members)-1)
	for m := range n.members {
		if m != id {
			members = append(members, m)
		}
	}
	n.mu.Unlock()
	_, err := n.propose(commandConfig, command{Members: members})
	if err == nil {
		n.mu.Lock()
		delete(n.members, id)
		n.mu.Unlock()
	}
	return err
}
func (n *Node) Members() []ID {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]ID, 0, len(n.members))
	for m := range n.members {
		out = append(out, m)
	}
	return out
}
func (n *Node) Leader() ID { n.mu.Lock(); defer n.mu.Unlock(); return n.leader }
func (n *Node) Role() Role { n.mu.Lock(); defer n.mu.Unlock(); return n.role }
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Role: n.role, Term: n.currentTerm, Leader: n.leader, CommitIndex: n.commitIndex, LastApplied: n.lastApplied, Snapshot: n.snapshotIndex, Members: n.memberSliceLocked()}
}
func (n *Node) memberSliceLocked() []ID {
	out := make([]ID, 0, len(n.members))
	for m := range n.members {
		out = append(out, m)
	}
	return out
}
func (n *Node) Snapshot() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.commitIndex <= n.snapshotIndex {
		return nil
	}
	data, err := json.Marshal(snapshotState{KV: n.kv, Members: n.memberSliceLocked()})
	if err != nil {
		return err
	}
	term := n.termAtLocked(n.commitIndex)
	cut := n.commitIndex - n.snapshotIndex
	n.log = cloneEntries(n.log[cut:])
	n.snapshotIndex, n.snapshotTerm = n.commitIndex, term
	n.storage.SetSnapshot(Snapshot{n.snapshotIndex, n.snapshotTerm, data})
	n.storage.SetLog(n.log)
	return nil
}
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
