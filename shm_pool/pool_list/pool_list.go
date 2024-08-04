package pool_list

import (
	"fmt"
	"unsafe"
)

type PoolListItem[T any] struct {
	value *T
	next  *PoolListItem[T]
}

type PoolList[T any] struct {
	head *PoolListItem[T]
	tail *PoolListItem[T]
	size int
}

func NewPoolList[T any](ptr *T, size int) (*PoolList[T], error) {
	if size <= 0 {
		panic("size must be greater than 0")
	}
	if ptr == nil {
		panic("p cannot be nil")
	}
	list := &PoolList[T]{
		head: nil,
		tail: nil,
		size: 0,
	}
	list.tail = list.head
	for i := 0; i < size; i++ {
		ptr2 := unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + unsafe.Sizeof(*ptr) * uintptr(i))
		list.Put((*T) (ptr2))
	}

	return list, nil
}

func (p *PoolList[T]) Get() (*T) {
	if p.head == nil {
		// panic("pool is empty")
		return nil
	}
	value := p.head.value
	if value == nil {
		panic(fmt.Sprintf("%p get value cannot be nil", p))
	}
	if p.head == p.tail {
		p.head = nil
		p.tail = nil
	} else {
		p.head = p.head.next
		if p.head == nil {
			p.tail = nil
		}
	}

	p.size--
	return value
}

func (p *PoolList[T]) Put(value *T) {
	if value == nil {
		panic(fmt.Sprintf("%p put value cannot be nil", p))
	}
	item := &PoolListItem[T]{
		value: value,
		next:  nil,
	}

	if p.tail == nil {
		p.head = item
		p.tail = item
	} else {
		p.tail.next = item
		p.tail = item
	}
	p.size++
}

func (p *PoolList[T]) GetLeftSize() int {
	return p.size
}

