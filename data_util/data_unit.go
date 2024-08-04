package data_util

import (
	"cmp"
	"time"
)

const (
	DataUnitOpTypeNone = iota
	DataUnitOpTypeSet
	DataUnitOpTypeAdd
	DataUnitOpTypeDel
)

type PoolData[K cmp.Ordered, T any] struct {
	Key      K
	Data     T
	UesShm   bool
	Recorded bool
	OpType   int // 操作类型
}

func (p *PoolData[K, T]) Set(Key K, data T) error {
	if p.OpType == DataUnitOpTypeDel {
		return NewDataErr(ErrCodeDataAlreadyDeleted)
	}
	p.Recorded = false
	p.Key = Key
	p.Data = data
	p.Recorded = true
	return nil
}

type DataLandingPtr[K cmp.Ordered, T any] struct {
	Dirty  bool            // 是否需要更新
	ShmPtr *PoolData[K, T] // 指向共享内存的指针
}

func (p *DataLandingPtr[K, T]) Set(Key K, data T) error {
	if p.ShmPtr == nil {
		return NewDataErr(ErrcodeShmMemIsNil)
	}
	err := p.ShmPtr.Set(Key, data)
	if err == nil {
		p.Dirty = true
	}
	return err
}

type DataPreparePtr[K cmp.Ordered, T any] struct {
	ShmPtr *PoolData[K, T] // 指向共享内存的指针
}

type DataUnit[T any, TSHM any] struct {
	Data    T
	ShmData TSHM

	op chan func(*T, *TSHM) error // 操作队列，如返回错误，则停止运行

	Landing   func(*DataUnit[T, TSHM]) // 数据落地
	DBLanding func(func())             // 放到datamgr中执行
}

func CreateDataUnit[T any, TSHM any](data T, shm_data TSHM, landing func(*DataUnit[T, TSHM]), db_landing func(func())) *DataUnit[T, TSHM] {
	du := &DataUnit[T, TSHM]{
		Data:      data,
		ShmData:   shm_data,
		op:        make(chan func(*T, *TSHM) error, 8),
		Landing:   landing,
		DBLanding: db_landing,
	}

	go func() {
		for {
			select {
			case op := <-du.op:
				if err := op(&du.Data, &du.ShmData); err != nil {
					// 处理错误
					if _e, ok := err.(*DataErr); ok {
						if _e.Errcode == ErrCodeDeconstruct {
							return
						}
					}
					// TODO: 记录错误日志
					panic(err)
					return
				}
				// du.record()
			case <-time.After(time.Second * 50):
				// 定时更新数据 将共享内存中的数据落地
				du.Landing(du)
			}
		}
	}()
	return du
}
