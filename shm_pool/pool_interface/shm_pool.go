package pool_interface

import (
	"fmt"
	"goshm/shm_pool/pool_list"
	"goshm/shm_pool/share_mem"
)

type ShmPool[T any] struct {
	m *share_mem.ShareMem[T]
	l *pool_list.PoolList[T]
}

func CreateShmPool[T any](name string, id int8, size int) (*ShmPool[T], error) {
	m, err := share_mem.CreateShareMem[T](name, id, size)
	if err != nil {
		return nil, err
	}

	l, err := pool_list.NewPoolList[T](m.GetPtr(), size)
	if err != nil {
		return nil, err
	}

	fmt.Println(name, id, size)
	return &ShmPool[T]{m: m, l: l}, nil
}

func (p *ShmPool[T]) GetLeftSize() int {
	return p.l.GetLeftSize()
}

func (p *ShmPool[T]) Get() *T {
	// fmt.Printf("get %p\n", p.L)
	return p.l.Get()
}

func (p *ShmPool[T]) Put(t *T) {
	// fmt.Printf("put %p\n", p.L)
	p.l.Put(t)
}

func (p *ShmPool[T]) Close() {
	p.m.Close()
}
