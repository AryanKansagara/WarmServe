package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Worker struct {
	URL  string
	Stop func()
}
type Factory func(context.Context, string) (Worker, error)
type slot struct {
	model    string
	worker   Worker
	busy     bool
	starting bool
	last     time.Time
}
type Pool struct {
	mu      sync.Mutex
	slots   []*slot
	maximum int
	ttl     time.Duration
	factory Factory
	now     func() time.Time
	closed  bool
	cold    atomic.Uint64
	warm    atomic.Uint64
	evicted atomic.Uint64
	expired atomic.Uint64
}

func NewPool(maximum int, ttl time.Duration, factory Factory) *Pool {
	if maximum < 1 || ttl <= 0 {
		panic("invalid pool limits")
	}
	return &Pool{maximum: maximum, ttl: ttl, factory: factory, now: time.Now}
}
func (p *Pool) remove(target *slot) {
	for index, item := range p.slots {
		if item == target {
			p.slots = append(p.slots[:index], p.slots[index+1:]...)
			return
		}
	}
}
func (p *Pool) Acquire(ctx context.Context, model string) (*slot, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, false, errors.New("pool closed")
		}
		for _, item := range p.slots {
			if item.model == model && !item.busy {
				item.busy = true
				p.warm.Add(1)
				p.mu.Unlock()
				return item, false, nil
			}
		}
		var stale *slot
		if len(p.slots) >= p.maximum {
			for _, item := range p.slots {
				if !item.busy && (stale == nil || item.last.Before(stale.last)) {
					stale = item
				}
			}
		}
		if len(p.slots) < p.maximum || stale != nil {
			if stale != nil {
				p.remove(stale)
				p.evicted.Add(1)
			}
			item := &slot{model: model, busy: true, starting: true, last: p.now()}
			p.slots = append(p.slots, item)
			p.mu.Unlock()
			if stale != nil {
				stale.worker.Stop()
			}
			worker, err := p.factory(ctx, model)
			p.mu.Lock()
			if err != nil || p.closed {
				p.remove(item)
				p.mu.Unlock()
				if err == nil {
					worker.Stop()
					err = errors.New("pool closed during startup")
				}
				return nil, false, err
			}
			item.worker = worker
			item.starting = false
			p.cold.Add(1)
			p.mu.Unlock()
			return item, true, nil
		}
		p.mu.Unlock()
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}
}
func (p *Pool) Release(item *slot, healthy bool) {
	p.mu.Lock()
	present := false
	for _, candidate := range p.slots {
		if candidate == item {
			present = true
		}
	}
	if !present {
		p.mu.Unlock()
		return
	}
	if !healthy {
		p.remove(item)
		p.mu.Unlock()
		item.worker.Stop()
		return
	}
	item.busy = false
	item.last = p.now()
	p.mu.Unlock()
}
func (p *Pool) Expire() int {
	p.mu.Lock()
	var stopped []*slot
	for _, item := range append([]*slot{}, p.slots...) {
		if !item.busy && p.now().Sub(item.last) >= p.ttl {
			p.remove(item)
			stopped = append(stopped, item)
		}
	}
	p.expired.Add(uint64(len(stopped)))
	p.mu.Unlock()
	for _, item := range stopped {
		item.worker.Stop()
	}
	return len(stopped)
}
func (p *Pool) Size() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.slots) }
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	items := p.slots
	p.slots = nil
	p.mu.Unlock()
	for _, item := range items {
		if !item.starting {
			item.worker.Stop()
		}
	}
}
