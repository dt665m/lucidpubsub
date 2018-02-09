package lucidpubsub

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"cloud.google.com/go/pubsub"
)

type PayloadHandler func([]byte) error

type MemoryQueue struct {
	mu              sync.Mutex
	queue           map[int64]*pubsub.Message
	dispatch        PayloadHandler
	currentSequence int64
}

func NewMemoryQueue(p PayloadHandler, seedSequence int64) *MemoryQueue {
	return &MemoryQueue{
		queue:           make(map[int64]*pubsub.Message),
		currentSequence: seedSequence,
		dispatch:        p,
	}
}

func (o *MemoryQueue) Enqueue(ctx context.Context, m *pubsub.Message) error {
	seq, err := strconv.ParseInt(m.Attributes["sequence"], 10, 64)
	if err != nil {
		m.Nack()
		return fmt.Errorf("Sequence Parsing Error: %v", err)
	}

	if seq < o.currentSequence {
		//we already handled this event
		m.Ack()
		return nil
	}

	//serialize access to the queue
	o.mu.Lock()
	o.queue[seq] = m
	m.Ack()
	sortArr := []int64{}
	for k := range o.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})

	for _, key := range sortArr {
		if key == o.currentSequence {
			message := o.queue[key]
			if err := o.dispatch(message.Data); err != nil {
				o.mu.Unlock()
				return err
			}
			delete(o.queue, key)
			o.currentSequence++
		} else if key > o.currentSequence {
			break
		}
	}
	o.mu.Unlock()

	return nil
}

func (o *MemoryQueue) LatestAck() int64 {
	o.mu.Lock()
	if len(o.queue) == 0 {
		o.mu.Unlock()
		return o.currentSequence - 1
	}
	sortArr := []int64{}
	for k := range o.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})
	o.mu.Unlock()

	return sortArr[len(sortArr)-1]
}

func (o *MemoryQueue) Dump() {
	o.mu.Lock()
	if len(o.queue) == 0 {
		o.mu.Unlock()
		return
	}

	sortArr := []int64{}
	for k := range o.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})
	o.mu.Unlock()

	for i := 0; i < len(sortArr); i++ {
		if i > 0 && sortArr[i] != sortArr[i-1]+1 {
			fmt.Println("GAP!")
		}
		fmt.Println(sortArr[i])
	}
}
