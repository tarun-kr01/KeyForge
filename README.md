# DistributedKV

DistributedKV is a small, dependency-free Raft-backed in-memory key-value
store. It includes leader election, replicated commands, linearizable reads,
snapshots, membership changes, an in-process transport, and a chaos harness.
It is intended as an educational, deterministic testable implementation rather
than a replacement for a production storage engine.

```go
t := raft.NewMemoryTransport()
ids := []raft.ID{"a", "b", "c"}
a := raft.NewNode("a", ids, t, raft.Options{})
b := raft.NewNode("b", ids, t, raft.Options{})
c := raft.NewNode("c", ids, t, raft.Options{})
a.Start(); b.Start(); c.Start()
leader, _ := (&raft.Harness{Transport:t, Nodes:map[raft.ID]*raft.Node{"a":a,"b":b,"c":c}}).Leader(time.Second)
_ = leader.Put("language", "Go")
value, _, _ := leader.Get("language")
```

`MemoryTransport` exposes `Partition`, `Heal`, `SetDrop`, `SetDelay`, and
`FailNext` for deterministic failure injection. `go test ./...` and
`go vet ./...` validate the package.
