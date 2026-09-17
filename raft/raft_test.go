package raft

import (
	"fmt"
	"testing"
	"time"
)

func testOptions() Options {
	return Options{
		ElectionTimeout:   70 * time.Millisecond,
		HeartbeatInterval: 15 * time.Millisecond,
		OperationTimeout:  800 * time.Millisecond,
		Seed:              42,
	}
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestElectionAndReplication(t *testing.T) {
	h := NewHarness([]ID{"a", "b", "c"}, testOptions())
	h.Start()
	defer h.Stop()
	leader, err := h.Leader(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.Put("answer", "42"); err != nil {
		t.Fatal(err)
	}
	for _, n := range h.Nodes {
		n := n
		waitFor(t, time.Second, func() bool {
			return n.Status().LastApplied >= 1
		})
	}
	v, ok, err := leader.Get("answer")
	if err != nil || !ok || v != "42" {
		t.Fatalf("read after replication = %q, %v, %v", v, ok, err)
	}
}

func TestFailoverAndPartitionHealing(t *testing.T) {
	h := NewHarness([]ID{"a", "b", "c"}, testOptions())
	h.Start()
	defer h.Stop()
	old, err := h.Leader(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Put("before", "x"); err != nil {
		t.Fatal(err)
	}
	for id := range h.Nodes {
		if id != old.ID() {
			h.Partition(old.ID(), id)
		}
	}
	newLeader, err := h.Leader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLeader.ID() == old.ID() {
		t.Fatal("isolated leader remained leader")
	}
	if err := newLeader.Put("after", "y"); err != nil {
		t.Fatal(err)
	}
	for id := range h.Nodes {
		if id != old.ID() {
			h.Heal(old.ID(), id)
		}
	}
	waitFor(t, time.Second, func() bool {
		return old.Role() == Follower
	})
}

func TestSnapshotCompactsLog(t *testing.T) {
	h := NewHarness([]ID{"a", "b", "c"}, testOptions())
	h.Start()
	defer h.Stop()
	leader, err := h.Leader(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := leader.Put("k", string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if got := leader.Status().Snapshot; got == 0 {
		t.Fatal("snapshot index did not advance")
	}
	if _, ok, err := leader.Get("k"); err != nil || !ok {
		t.Fatalf("state was not retained by snapshot: %v", err)
	}
}

func TestMembershipChange(t *testing.T) {
	opts := testOptions()
	transport := NewMemoryTransport()
	ids := []ID{"a", "b", "c"}
	nodes := map[ID]*Node{}
	for _, id := range ids {
		nodes[id] = NewNode(id, ids, transport, opts)
	}
	// A member can be provisioned before its configuration entry is committed.
	nodes["d"] = NewNode("d", append(ids, "d"), transport, opts)
	for _, n := range nodes {
		if n.ID() != "d" {
			n.Start()
		}
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()
	leader, err := (&Harness{Transport: transport, Nodes: nodes}).Leader(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.Put("before-join", "replicated"); err != nil {
		t.Fatal(err)
	}
	if err := leader.AddMember("d"); err != nil {
		t.Fatal(err)
	}
	nodes["d"].Start()
	waitFor(t, time.Second, func() bool {
		return nodes["d"].Status().LastApplied >= leader.Status().LastApplied
	})
}

func TestChaosFailureInjection(t *testing.T) {
	h := NewHarness([]ID{"a", "b", "c"}, testOptions())
	h.Start()
	defer h.Stop()
	leader, err := h.Leader(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var peer ID
	for id := range h.Nodes {
		if id != leader.ID() {
			peer = id
			break
		}
	}
	h.Transport.FailNext(leader.ID(), peer, 2)
	if err := leader.Put("resilient", "yes"); err != nil {
		t.Fatal(err)
	}
	h.Transport.SetDrop(leader.ID(), peer, true)
	if err := leader.Put("quorum", "yes"); err != nil {
		t.Fatal(err)
	}
}

func TestWritesRemainConsistentAcrossRepeatedFailures(t *testing.T) {
	h := NewHarness([]ID{"a", "b", "c", "d", "e"}, testOptions())
	h.Start()
	defer h.Stop()

	for i := 0; i < 20; i++ {
		leader, err := h.Leader(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("key-%02d", i)
		if err := leader.Put(key, "committed"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if i%4 == 3 {
			for id := range h.Nodes {
				if id != leader.ID() {
					h.Kill(id)
					if err := h.Restart(id); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		leader, err := h.Leader(time.Second)
		if err != nil {
			return false
		}
		value, found, err := leader.Get("key-19")
		return err == nil && found && value == "committed"
	})
}
