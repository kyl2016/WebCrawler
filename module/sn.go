package module

import (
	"math"
	"sync"
)

type SNGenerator interface {
	Start() uint64
	Max() uint64
	Next() uint64
	CycleCount() uint64
	Get() uint64
}

type mySNGenerator struct {
	start      uint64
	max        uint64
	next       uint64
	cycleCount uint64
	lock       sync.RWMutex
}

func NewSNGenerator(start uint64, max uint64) SNGenerator {
	if max == 0 {
		max = math.MaxUint64
	}

	return &mySNGenerator{
		start: start,
		max:   max,
		next:  start,
	}
}

func (gen *mySNGenerator) Start() uint64 {
	return gen.start
}

func (gen *mySNGenerator) Max() uint64 {
	return gen.max
}

func (gen *mySNGenerator) Next() uint64 {
	gen.lock.RLock()
	defer gen.lock.RUnlock()
	return gen.next
}

func (gen *mySNGenerator) CycleCount() uint64 {
	gen.lock.RLock()
	defer gen.lock.RUnlock()
	return gen.cycleCount
}

func (gen *mySNGenerator) Get() uint64 {
	gen.lock.Lock()
	defer gen.lock.Unlock()
	id := gen.next
	if id == gen.max {
		gen.next = gen.start
		gen.cycleCount++
	} else {
		gen.next++
	}

	return id
}
