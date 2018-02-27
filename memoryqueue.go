package lucidpubsub

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

type MemoryQueue struct {
	mu              sync.Mutex
	currentSequence int64
	queue           map[int64][]byte
}

type iterator struct {
	*MemoryQueue
	sortArr []int64
	index   int
}

// Next
func (i *iterator) Next() []byte {
	data := i.queue[i.currentSequence]
	delete(i.queue, i.currentSequence)
	i.currentSequence++
	i.index++
	return data
}

// HasNext Iterate queue and returns true if available, false if empty
func (i *iterator) HasNext() bool {
	return i.index < len(i.sortArr) && i.sortArr[i.index] == i.currentSequence
}

// NewMemoryQueue create new MemoryQueue
func NewMemoryQueue(seedSequence int64) *MemoryQueue {
	return &MemoryQueue{
		currentSequence: seedSequence,
		queue:           make(map[int64][]byte),
	}
}

// NewIterator create new iterator on items in the queue
func (m *MemoryQueue) NewIterator() *iterator {
	sortArr := []int64{}
	for k := range m.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})
	return &iterator{
		MemoryQueue: m,
		sortArr:     sortArr,
	}
}

// Enqueue message for ordering
func (m *MemoryQueue) Enqueue(seq int64, data []byte) error {
	//serialize access to the queue
	m.mu.Lock()
	if seq < m.currentSequence {
		//we already handled this event
		return errors.New("Sequence Already Handled")
	}
	m.queue[seq] = data
	m.mu.Unlock()

	return nil
}

// LatestAck get the latest item acked
func (m *MemoryQueue) LatestAck() int64 {
	m.mu.Lock()
	if len(m.queue) == 0 {
		m.mu.Unlock()
		return m.currentSequence - 1
	}
	sortArr := []int64{}
	for k := range m.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})
	m.mu.Unlock()

	return sortArr[len(sortArr)-1]
}

// Dump debug info
func (m *MemoryQueue) Dump() {
	m.mu.Lock()
	if len(m.queue) == 0 {
		m.mu.Unlock()
		return
	}

	sortArr := []int64{}
	for k := range m.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})
	m.mu.Unlock()

	for i := 0; i < len(sortArr); i++ {
		if i > 0 && sortArr[i] != sortArr[i-1]+1 {
			fmt.Println("GAP!")
		}
		fmt.Println(sortArr[i])
	}
}
