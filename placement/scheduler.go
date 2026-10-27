// Package placement models centralized multi-resource placement; it is not a
// distributed coordinator and does not execute workloads on real nodes.
package placement

import (
	"errors"
	"sort"
	"sync"
)

type Resources struct{ CPU, Memory, GPU int64 } // millicores, MiB, devices
type Node struct {
	ID        string
	Capacity  Resources
	Available bool
}
type Reservation struct {
	Job, Node string
	Request   Resources
}
type State struct {
	Node Node
	Used Resources
}
type Scheduler struct {
	mu    sync.Mutex
	nodes map[string]Node
	used  map[string]Resources
	jobs  map[string]Reservation
}

func valid(r Resources) bool {
	return r.CPU > 0 && r.Memory > 0 && r.GPU >= 0 && r.CPU <= 1_000_000 && r.Memory <= 1_000_000_000 && r.GPU <= 1024
}
func fits(used, request, capacity Resources) bool {
	return used.CPU+request.CPU <= capacity.CPU && used.Memory+request.Memory <= capacity.Memory && used.GPU+request.GPU <= capacity.GPU
}
func New(nodes []Node) (*Scheduler, error) {
	s := &Scheduler{nodes: map[string]Node{}, used: map[string]Resources{}, jobs: map[string]Reservation{}}
	for _, node := range nodes {
		if node.ID == "" || !valid(node.Capacity) {
			return nil, errors.New("invalid node")
		}
		if _, exists := s.nodes[node.ID]; exists {
			return nil, errors.New("duplicate node")
		}
		s.nodes[node.ID] = node
	}
	if len(s.nodes) == 0 {
		return nil, errors.New("empty capacity inventory")
	}
	return s, nil
}

// Reserve atomically filters nodes and chooses the least remaining normalized
// CPU+memory slack. This is a greedy packing heuristic, not a global optimum.
func (s *Scheduler) Reserve(job string, request Resources) (Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job == "" || !valid(request) {
		return Reservation{}, errors.New("invalid request")
	}
	if prior, exists := s.jobs[job]; exists {
		if prior.Request != request {
			return Reservation{}, errors.New("conflicting job identifier")
		}
		return prior, nil
	}
	bestID := ""
	var bestScore int64
	for id, node := range s.nodes {
		used := s.used[id]
		if !node.Available || !fits(used, request, node.Capacity) {
			continue
		}
		score := (node.Capacity.CPU-used.CPU-request.CPU)*1_000_000/node.Capacity.CPU +
			(node.Capacity.Memory-used.Memory-request.Memory)*1_000_000/node.Capacity.Memory
		// Preserve accelerator nodes for jobs that actually request a GPU.
		if request.GPU == 0 && node.Capacity.GPU > 0 {
			score += 3_000_000
		}
		if bestID == "" || score < bestScore || (score == bestScore && id < bestID) {
			bestID, bestScore = id, score
		}
	}
	if bestID == "" {
		return Reservation{}, errors.New("no feasible capacity")
	}
	used := s.used[bestID]
	s.used[bestID] = Resources{used.CPU + request.CPU, used.Memory + request.Memory, used.GPU + request.GPU}
	reservation := Reservation{job, bestID, request}
	s.jobs[job] = reservation
	return reservation, nil
}

func (s *Scheduler) Release(job string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	reservation, exists := s.jobs[job]
	if !exists {
		return false
	}
	used, r := s.used[reservation.Node], reservation.Request
	s.used[reservation.Node] = Resources{used.CPU - r.CPU, used.Memory - r.Memory, used.GPU - r.GPU}
	delete(s.jobs, job)
	return true
}

// Availability changes stop new placements, without silently migrating jobs.
func (s *Scheduler) SetAvailable(id string, available bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, exists := s.nodes[id]
	if !exists {
		return errors.New("unknown node")
	}
	node.Available = available
	s.nodes[id] = node
	return nil
}
func (s *Scheduler) Snapshot() []State {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]State, 0, len(s.nodes))
	for id, node := range s.nodes {
		result = append(result, State{node, s.used[id]})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Node.ID < result[j].Node.ID })
	return result
}
