package placement

import (
	"fmt"
	"sync"
	"testing"
)

func inventory(t *testing.T) *Scheduler {
	t.Helper()
	s, err := New([]Node{{"cpu", Resources{1000, 1024, 0}, true}, {"gpu", Resources{4000, 8192, 1}, true}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestBestFit(t *testing.T) {
	s := inventory(t)
	r, err := s.Reserve("a", Resources{500, 128, 0})
	if err != nil || r.Node != "cpu" {
		t.Fatalf("placement: %+v %v", r, err)
	}
}
func TestGPUConstraint(t *testing.T) {
	r, err := inventory(t).Reserve("a", Resources{500, 128, 1})
	if err != nil || r.Node != "gpu" {
		t.Fatalf("placement: %+v %v", r, err)
	}
}
func TestGPUExhaustion(t *testing.T) {
	s := inventory(t)
	_, _ = s.Reserve("a", Resources{500, 128, 1})
	if _, err := s.Reserve("b", Resources{500, 128, 1}); err == nil {
		t.Fatal("GPU overcommit")
	}
}
func TestMemoryConstraint(t *testing.T) {
	r, err := inventory(t).Reserve("a", Resources{500, 2000, 0})
	if err != nil || r.Node != "gpu" {
		t.Fatalf("placement: %+v %v", r, err)
	}
}
func TestReleaseIdempotent(t *testing.T) {
	s := inventory(t)
	_, _ = s.Reserve("a", Resources{500, 128, 0})
	if !s.Release("a") || s.Release("a") {
		t.Fatal("release semantics")
	}
	for _, state := range s.Snapshot() {
		if state.Used != (Resources{}) {
			t.Fatal("capacity leaked")
		}
	}
}
func TestDuplicateReservation(t *testing.T) {
	s := inventory(t)
	a, _ := s.Reserve("a", Resources{500, 128, 0})
	b, _ := s.Reserve("a", Resources{500, 128, 0})
	if a != b || s.Snapshot()[0].Used.CPU != 500 {
		t.Fatal("duplicate allocation")
	}
}
func TestConflictingJob(t *testing.T) {
	s := inventory(t)
	_, _ = s.Reserve("a", Resources{500, 128, 0})
	if _, err := s.Reserve("a", Resources{100, 128, 0}); err == nil {
		t.Fatal("conflict accepted")
	}
}
func TestUnavailableNode(t *testing.T) {
	s := inventory(t)
	_ = s.SetAvailable("cpu", false)
	r, err := s.Reserve("a", Resources{500, 128, 0})
	if err != nil || r.Node != "gpu" {
		t.Fatal("unavailable node used")
	}
	if err := s.SetAvailable("absent", true); err == nil {
		t.Fatal("unknown node")
	}
}
func TestInvalidInputs(t *testing.T) {
	for _, nodes := range [][]Node{nil, {{"x", Resources{}, true}}, {{"x", Resources{1, 1, 0}, true}, {"x", Resources{1, 1, 0}, true}}} {
		if _, err := New(nodes); err == nil {
			t.Fatal("bad inventory accepted")
		}
	}
	s := inventory(t)
	for _, request := range []Resources{{0, 1, 0}, {1, -1, 0}, {1, 1, -1}, {1 << 60, 1, 0}} {
		if _, err := s.Reserve("a", request); err == nil {
			t.Fatal("bad request accepted")
		}
	}
}
func TestStableTieBreak(t *testing.T) {
	s, _ := New([]Node{{"b", Resources{1000, 1024, 0}, true}, {"a", Resources{1000, 1024, 0}, true}})
	r, _ := s.Reserve("job", Resources{100, 128, 0})
	if r.Node != "a" {
		t.Fatal("nondeterministic tie")
	}
}
func TestSnapshotCopy(t *testing.T) {
	s := inventory(t)
	snapshot := s.Snapshot()
	snapshot[0].Used.CPU = 9999
	if s.Snapshot()[0].Used.CPU != 0 {
		t.Fatal("mutable alias")
	}
}
func TestConcurrentCapacityStress(t *testing.T) {
	var nodes []Node
	for i := 0; i < 5; i++ {
		nodes = append(nodes, Node{fmt.Sprint(i), Resources{2000, 4096, 0}, true})
	}
	s, _ := New(nodes)
	accepted := make(chan string, 200)
	var workers sync.WaitGroup
	for i := 0; i < 200; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			id := fmt.Sprint(i)
			if _, err := s.Reserve(id, Resources{100, 128, 0}); err == nil {
				accepted <- id
			}
		}(i)
	}
	workers.Wait()
	close(accepted)
	if len(accepted) != 100 {
		t.Fatalf("accepted %d, expected CPU-limited 100", len(accepted))
	}
	for _, state := range s.Snapshot() {
		if !fits(Resources{}, state.Used, state.Node.Capacity) {
			t.Fatal("overcommit")
		}
	}
	for id := range accepted {
		s.Release(id)
	}
	for _, state := range s.Snapshot() {
		if state.Used != (Resources{}) {
			t.Fatal("unreclaimed reservation")
		}
	}
	t.Log("200 concurrent requests, 5 simulated nodes, 100 feasible reservations, no overcommit, all capacity reclaimed")
}
