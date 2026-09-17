package raft

import (
	"sync"
	"time"
)

// Transport is the only networking dependency of a Node. Implementations may
// be backed by TCP, while MemoryTransport is deterministic and test friendly.
type Transport interface {
	Register(ID, *Node)
	Unregister(ID)
	SendRequestVote(ID, ID, RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ID, ID, AppendEntriesArgs) (AppendEntriesReply, error)
	SendInstallSnapshot(ID, ID, InstallSnapshotArgs) (InstallSnapshotReply, error)
}

// MemoryTransport connects nodes in-process. Link controls make partitions,
// packet loss, latency, and mid-write failures straightforward to reproduce.
type MemoryTransport struct {
	mu       sync.RWMutex
	nodes    map[ID]*Node
	links    map[ID]map[ID]bool
	drop     map[ID]map[ID]bool
	delay    time.Duration
	failNext map[ID]map[ID]int
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{nodes: make(map[ID]*Node), links: make(map[ID]map[ID]bool), drop: make(map[ID]map[ID]bool), failNext: make(map[ID]map[ID]int)}
}
func (t *MemoryTransport) Register(id ID, n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = n
	if t.links[id] == nil {
		t.links[id] = make(map[ID]bool)
	}
	for other := range t.nodes {
		if t.links[other] == nil {
			t.links[other] = make(map[ID]bool)
		}
		t.links[id][other], t.links[other][id] = true, true
	}
}
func (t *MemoryTransport) Unregister(id ID) { t.mu.Lock(); delete(t.nodes, id); t.mu.Unlock() }
func (t *MemoryTransport) SetLink(a, b ID, enabled bool) {
	t.mu.Lock()
	if t.links[a] == nil {
		t.links[a] = make(map[ID]bool)
	}
	if t.links[b] == nil {
		t.links[b] = make(map[ID]bool)
	}
	t.links[a][b], t.links[b][a] = enabled, enabled
	t.mu.Unlock()
}
func (t *MemoryTransport) Partition(a, b ID) { t.SetLink(a, b, false) }
func (t *MemoryTransport) Heal(a, b ID)      { t.SetLink(a, b, true) }
func (t *MemoryTransport) SetDrop(a, b ID, enabled bool) {
	t.mu.Lock()
	if t.drop[a] == nil {
		t.drop[a] = make(map[ID]bool)
	}
	t.drop[a][b] = enabled
	t.mu.Unlock()
}
func (t *MemoryTransport) SetDelay(d time.Duration) { t.mu.Lock(); t.delay = d; t.mu.Unlock() }
func (t *MemoryTransport) Reachable(id ID) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	count := 0
	for peer := range t.nodes {
		if t.links[id][peer] && !t.drop[id][peer] {
			count++
		}
	}
	return count
}
func (t *MemoryTransport) FailNext(a, b ID, count int) {
	t.mu.Lock()
	if t.failNext[a] == nil {
		t.failNext[a] = make(map[ID]int)
	}
	t.failNext[a][b] = count
	t.mu.Unlock()
}

func (t *MemoryTransport) endpoint(from, to ID) (*Node, time.Duration, error) {
	t.mu.Lock()
	n := t.nodes[to]
	ok := n != nil && t.links[from][to] && !t.drop[from][to]
	if c := t.failNext[from][to]; c > 0 {
		t.failNext[from][to] = c - 1
		ok = false
	}
	d := t.delay
	t.mu.Unlock()
	if !ok {
		return nil, 0, ErrNoQuorum
	}
	return n, d, nil
}
func (t *MemoryTransport) SendRequestVote(from, to ID, a RequestVoteArgs) (RequestVoteReply, error) {
	n, d, err := t.endpoint(from, to)
	if err != nil {
		return RequestVoteReply{}, err
	}
	if d > 0 {
		time.Sleep(d)
	}
	return n.handleRequestVote(a), nil
}
func (t *MemoryTransport) SendAppendEntries(from, to ID, a AppendEntriesArgs) (AppendEntriesReply, error) {
	n, d, err := t.endpoint(from, to)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	if d > 0 {
		time.Sleep(d)
	}
	return n.handleAppendEntries(a), nil
}
func (t *MemoryTransport) SendInstallSnapshot(from, to ID, a InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n, d, err := t.endpoint(from, to)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	if d > 0 {
		time.Sleep(d)
	}
	return n.handleInstallSnapshot(a), nil
}
