package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// Harness is a deterministic chaos-test helper for in-memory clusters.
type Harness struct {
	Transport *MemoryTransport
	Nodes     map[ID]*Node
	rng       *rand.Rand
}

func NewHarness(ids []ID, opts Options) *Harness {
	t := NewMemoryTransport()
	h := &Harness{Transport: t, Nodes: map[ID]*Node{}, rng: rand.New(rand.NewSource(opts.Seed))}
	for _, id := range ids {
		h.Nodes[id] = NewNode(id, ids, t, opts)
	}
	return h
}
func (h *Harness) Start() {
	for _, n := range h.Nodes {
		n.Start()
	}
}
func (h *Harness) Stop() {
	for _, n := range h.Nodes {
		n.Stop()
	}
}
func (h *Harness) Leader(timeout time.Duration) (*Node, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		counts := make(map[ID]int)
		for _, n := range h.Nodes {
			status := n.Status()
			if status.Leader != "" {
				counts[status.Leader]++
			}
		}
		for id, count := range counts {
			if count >= len(h.Nodes)/2+1 && h.Nodes[id] != nil && h.Nodes[id].Role() == Leader &&
				h.Transport.Reachable(id) >= len(h.Nodes)/2+1 {
				return h.Nodes[id], nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, ErrTimeout
}
func (h *Harness) Partition(a, b ID) { h.Transport.Partition(a, b) }
func (h *Harness) Heal(a, b ID)      { h.Transport.Heal(a, b) }
func (h *Harness) Kill(id ID) {
	if n := h.Nodes[id]; n != nil {
		n.Stop()
	}
}
func (h *Harness) Restart(id ID) error {
	n := h.Nodes[id]
	if n == nil {
		return fmt.Errorf("unknown node %s", id)
	}
	n.Start()
	return nil
}
func (h *Harness) RandomFailure() {
	ids := make([]ID, 0, len(h.Nodes))
	for id := range h.Nodes {
		ids = append(ids, id)
	}
	if len(ids) > 0 {
		h.Kill(ids[h.rng.Intn(len(ids))])
	}
}
