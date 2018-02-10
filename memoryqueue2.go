package lucidpubsub

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

type PayloadHandler func([]byte) error

type MemoryQueue2 struct {
	mu              sync.Mutex
	currentSequence int64
	queue           map[int64][]byte
}

type Iterator struct {
	mem   *MemoryQueue2
	sortArr []int64
	index int
}

func (i *Iterator) Next() []byte {
	if i.index < len(i.sortArr) {
		key := i.sortArr[index]
		if key == i.mem.currentSequence {
			data := i.mem.queue[key]
			delete(i.mem.queue[key])
			o.mem.currentSequence++
			i.index++
			return data
		}
	}
	return nil
}

func NewMemoryQueue2(seedSequence int64) *MemoryQueue2 {
	return &MemoryQueue2{
		currentSequence: seedSequence,
		queue:           make(map[int64][]byte),
	}
}

func (o *MemoryQueue2) NewIterator() *Iterator {
	sortArr := []int64{}
	for k := range o.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})
	return &Iterator{
		queue:   o,
		sortArr: sortArr,
	}
}

func (o *MemoryQueue2) Enqueue(seq int64, data []byte) error {
	//serialize access to the queue
	o.mu.Lock()
	if seq < o.currentSequence {
		//we already handled this event
		return errors.New("Sequence Already Handled")
	}
	o.queue[seq] = data
	o.mu.Unlock()

	return nil
}

func (o *MemoryQueue2) Process(handler PayloadHandler) error {
	o.mu.Lock()

	sortArr := []int64{}
	for k := range o.queue {
		sortArr = append(sortArr, k)
	}
	sort.Slice(sortArr, func(i, j int) bool {
		return sortArr[i] < sortArr[j]
	})

	for _, key := range sortArr {
		if key == o.currentSequence {
			data := o.queue[key]
			if err := o.dispatch(data); err != nil {
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
}

func (o *MemoryQueue2) LatestAck() int64 {
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

func (o *MemoryQueue2) Dump() {
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
