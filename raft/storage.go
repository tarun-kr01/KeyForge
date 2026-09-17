package raft

import "sync"

// Snapshot is a complete state-machine image.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// MemoryStorage is a small, thread-safe volatile storage implementation. It
// deliberately mirrors the durable-storage boundary, making it easy to swap
// in a disk implementation without changing Node.
type MemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor ID
	log      []LogEntry
	snapshot Snapshot
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) State() (uint64, ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.votedFor
}
func (s *MemoryStorage) SetState(term uint64, vote ID) {
	s.mu.Lock()
	s.term, s.votedFor = term, vote
	s.mu.Unlock()
}
func (s *MemoryStorage) Log() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneEntries(s.log)
}
func (s *MemoryStorage) SetLog(entries []LogEntry) {
	s.mu.Lock()
	s.log = cloneEntries(entries)
	s.mu.Unlock()
}
func (s *MemoryStorage) GetSnapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.snapshot
	out.Data = append([]byte(nil), out.Data...)
	return out
}
func (s *MemoryStorage) SetSnapshot(snapshot Snapshot) {
	s.mu.Lock()
	snapshot.Data = append([]byte(nil), snapshot.Data...)
	s.snapshot = snapshot
	s.mu.Unlock()
}

func cloneEntries(in []LogEntry) []LogEntry {
	out := make([]LogEntry, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Data = append([]byte(nil), in[i].Data...)
	}
	return out
}
