package data_util

import (
	"cmp"
	"fmt"

	"github.com/724789975/go_shm_data/lru_cache"
)

var printTest = false

type opStruct[K cmp.Ordered, V any, TSHM any] struct {
	cb func(K, error)
	f  func(*V, *TSHM) error
}

type reuseEliminate[V any, TSHM any] struct {
	h     int
	fList []func(*DataUnit[V, TSHM])
}

// 创建后便不可销毁
type DataMgr[K cmp.Ordered, V any, TSHM any] struct {
	lru_cache         *lru_cache.LRUCache[K, *DataUnit[V, TSHM]]
	load_data_func    func(K) (V, error)
	load_func         func(*DataMgr[K, V , TSHM])
	fetch_shm_func    func() (TSHM, error)
	release_shm_func  func(*TSHM)
	data_del_func     func(*DataUnit[V, TSHM])
	data_load_func    func(*V, *TSHM)
	data_create_func  func(k K, v V) error
	data_landing_func func(*DataUnit[V, TSHM])
	load_list         [64]chan func()
	op_list           chan func()
	wait_eliminate    map[K]*DataUnit[V, TSHM]
	after_eliminate   map[K]*reuseEliminate[V, TSHM]
	data_op_list      map[K][]*opStruct[K, V, TSHM]
}

/**
 * 创建数据管理器
 * @param max_size 最大缓存大小
 * @param load_data_func 加载数据
 * @param load_func 加载函数(加载共享内存,并初始化数据)
 * @param fetch_shm_func 获取共享内存
 * @param release_shm_func 释放共享内存
 * @param data_del_func 数据删除
 * @param data_load_func 数据加载
 * @param data_create_func 数据创建(直接落地到db,成功则说明创建成功,失败则说明数据落地失败)
 * @param landing_func 数据落地
 * @return 数据管理器
 */
func CreateDataMgr[K cmp.Ordered, V any, TSHM any](max_size int,
	load_data_func func(K) (V, error),
	load_func func(*DataMgr[K, V , TSHM]),
	fetch_shm_func func() (TSHM, error),
	release_shm_func func(*TSHM),
	data_del_func func(*DataUnit[V, TSHM]),
	data_load_func func(*V, *TSHM),
	data_create_func func(k K, v V) error,
	data_landing_func func(*DataUnit[V, TSHM]),
) *DataMgr[K, V, TSHM] {
	dm := &DataMgr[K, V, TSHM]{
		load_data_func:    load_data_func,
		load_func:         load_func,
		fetch_shm_func:    fetch_shm_func,
		release_shm_func:  release_shm_func,
		data_del_func:     data_del_func,
		data_load_func:    data_load_func,
		data_create_func:  data_create_func,
		data_landing_func: data_landing_func,
		wait_eliminate:    make(map[K]*DataUnit[V, TSHM]),
		after_eliminate:   make(map[K]*reuseEliminate[V, TSHM]),
		data_op_list:      make(map[K][]*opStruct[K, V, TSHM]),
	}

	//淘汰处理
	dm.lru_cache = lru_cache.CreateLRUCache(max_size, func(k K, du *DataUnit[V, TSHM]) {
		if printTest {
			fmt.Printf("will eliminate data: %v\n", k)
		}
		if _, ok := dm.wait_eliminate[k]; ok {
			panic("data is already being eliminated")
		}
		ch := make(chan func())
		go func() {
			f := <-ch
			f()
		}()
		dm.wait_eliminate[k] = du
		du.op <- func(v *V, shm *TSHM) error {
			if printTest {
				fmt.Printf("will eliminate data for landing : %v\n", k)
			}
			du.Landing(du)
			ch <- func() {
				dm.op_list <- func() {
					if printTest {
						fmt.Printf("do eliminate data: %v\n", k)
					}
					if _, ok := dm.wait_eliminate[k]; !ok {
						panic("data is not being eliminated")
					}
					if printTest {
						fmt.Printf("do eliminate data for release: %v\n", k)
					}
					if v, ok := dm.after_eliminate[k]; ok {
						dm.data_load_func(&du.Data, shm)
						_du := CreateDataUnit(du.Data, du.ShmData, dm.data_landing_func, dm.getDataLandingFunc(v.h))
						for _, f := range v.fList {
							f(_du)
						}
						dm.lru_cache.Put(k, _du)
						delete(dm.after_eliminate, k)
					} else {
						dm.release_shm_func(&du.ShmData)
					}
					delete(dm.wait_eliminate, k)
				}
			}
			return NewDataErr(ErrCodeDeconstruct)
		}
	})

	for i := 0; i < len(dm.load_list); i++ {
		dm.load_list[i] = make(chan func(), 256)
		ch := dm.load_list[i]
		go func() {
			for {
				f := <-ch
				f()
			}
		}()
	}

	dm.op_list = make(chan func(), 1)
	go func() {
		for {
			f := <-dm.op_list
			f()
		}
	}()

	dm.op_list <- func() {
		dm.load_func(dm)
	}
	return dm
}

func calcDBIndex(key int, size int) int {
	if size == 0 {
		panic("size can not be zero")
	}
	return (((key & 0xAAAAAAAA) >> 1) | ((key & 0x55555555) << 1)) % size
}

const (
	MaxNodeID = 16384
)

func crc16(data []byte) uint16 {
	crc16tab := [256]uint16{
		0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
		0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
		0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
		0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
		0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
		0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
		0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
		0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
		0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
		0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
		0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
		0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
		0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
		0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
		0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
		0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
		0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
		0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
		0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
		0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
		0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
		0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
		0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
		0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
		0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
		0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
		0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
		0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
		0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
		0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
		0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
		0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
	}
	crc := uint16(0)
	for _, b := range data {
		crc = (crc << 8) ^ crc16tab[(crc>>8)^uint16(b)&0xff]
	}
	return crc & (MaxNodeID - 1)
}

func GetCRCString(id string) uint16 {
	s := 0
	e := 0
	for s < len(id) {
		if id[s] == '{' {
			break
		}
		s++
	}
	if s == len(id) {
		return crc16([]byte(id))
	}
	for e = s + 1; e < len(id); e++ {
		if id[e] == '}' {
			break
		}
	}
	if e == len(id) || e == s+1 {
		return crc16([]byte(id))
	}
	return crc16([]byte(id[s+1 : e]))
}

func (dm *DataMgr[K, V, TSHM]) GetCacheSize() int {
	return dm.lru_cache.Size()
}

func (dm *DataMgr[K, V, TSHM]) getDataLandingFunc(h int) func(func()) {
	idx := calcDBIndex(h, len(dm.load_list))
	return func(f func()) {
		dm.load_list[idx] <- f
	}
}

// 将数据加载到缓存中 只会在load_func中调用
func (dm *DataMgr[K, V, TSHM]) PushData(k K, h int, v V, shm TSHM) {
	du := CreateDataUnit(v, shm, dm.data_landing_func, dm.getDataLandingFunc(h))
	dm.lru_cache.Put(k, du)
}

// 上层保证key的唯一性
func (dm *DataMgr[K, V, TSHM]) NewData(k K, h int, v V, cb func(V, error)) {
	dm.op_list <- func() {

		if _, ok := dm.lru_cache.Get(k); ok {
			//已有数据
			cb(v, NewDataErr(ErrCodeDataAlreadyExists))
			return
		}

		if _, ok := dm.wait_eliminate[k]; ok {
			//已有数据
			cb(v, NewDataErr(ErrCodeDataAlreadyExists))
			return
		}

		ch := make(chan func())
		dm.getDataLandingFunc(h)(func() {
			// 在load_list中加载数据
			if err := dm.data_create_func(k, v); err != nil {
				ch <- func() {
					cb(v, err)
				}
				return
			}

			ch <- func() {
				shm, err := dm.fetch_shm_func()
				if err != nil {
					cb(v, err)
					return
				}
				dm.data_load_func(&v, &shm)
				du := CreateDataUnit(v, shm, dm.data_landing_func, dm.getDataLandingFunc(h))
				dm.lru_cache.Put(k, du)
				cb(du.Data, nil)
			}
		})

		go func() {
			f := <-ch
			dm.op_list <- f
		}()
	}
}

func (dm *DataMgr[K, V, TSHM]) DelData(k K, f func(K, *V) error) {
	dm.op_list <- func() {
		if du, ok := dm.lru_cache.Get(k); !ok {
			//TODO: 必然有的 没有的话 记个日志
			return
		} else {
			du.op <- func(v *V, shm *TSHM) error {
				dm.data_del_func(du)
				f(k, &du.Data)
				return NewDataErr(ErrCodeDeconstruct)
			}
		}
	}
}

/**
 * 操作数据
 * @param key 数据的key
 * @param h 数据的hash值
 * @param cb 回调函数,只通知f投递的结果，不关心具体执行
 * @param f 操作数据的函数
 */
func (dm *DataMgr[K, V, TSHM]) OpData(key K, h int, cb func(K, error), f func(*V, *TSHM) error) {
	c := make(chan func())
	dm.op_list <- func() {
		d, ok := dm.lru_cache.Get(key)
		if ok {
			d.op <- f
			c <- func() {
				cb(key, nil)
			}
			return
		}
		if _, ok := dm.wait_eliminate[key]; ok {
			if _, ok := dm.after_eliminate[key]; !ok {
				dm.after_eliminate[key] = &reuseEliminate[V, TSHM]{h: h, fList: make([]func(*DataUnit[V, TSHM]), 0)}
			}

			dm.after_eliminate[key].fList = append(dm.after_eliminate[key].fList, func(_du *DataUnit[V, TSHM]) {
				_du.op <- f
				c <- func() {
					cb(key, nil)
				}
			})
			return
		}

		if _, ok := dm.data_op_list[key]; !ok {
			_shm, err := dm.fetch_shm_func()
			if err != nil {
				c <- func() {
					cb(key, err)
				}
				return
			}
			dm.data_op_list[key] = make([]*opStruct[K, V, TSHM], 0)
			dm.data_op_list[key] = append(dm.data_op_list[key], &opStruct[K, V, TSHM]{cb: cb, f: f})
			c <- func() {
				// fmt.Printf("load data 111111111111111 %v\n", key)
				dm.load_list[calcDBIndex(h, len(dm.load_list))] <- func() {
					d, err := dm.load_data_func(key)
					// fmt.Printf("load data 222222222222222 %v\n", key)
					if err != nil {
						// fmt.Printf("load data 333333333333333 %v\n", key)
						//记录加载失败的情况 默认加载都是成功的
						dm.op_list <- func() {
							for _, op := range dm.data_op_list[key] {
								op.cb(key, err)
							}
							delete(dm.data_op_list, key)
						}
						return
					}

					dm.op_list <- func() {
						// fmt.Printf("load data 444444444444444 %v\n", key)
						dm.data_load_func(&d, &_shm)
						du := CreateDataUnit(d, _shm, dm.data_landing_func, dm.getDataLandingFunc(h))
						for _, op := range dm.data_op_list[key] {
							du.op <- op.f
							op.cb(key, nil)
						}
						// fmt.Printf("load data 555555555555555 %v\n", key)
						delete(dm.data_op_list, key)
						dm.lru_cache.Put(key, du)
					}
				}
			}
			return
		}
		dm.data_op_list[key] = append(dm.data_op_list[key], &opStruct[K, V, TSHM]{cb: cb, f: f})
		c <- func() {
			// fmt.Printf("wait for load data %v\n", key)
		}

	}
	f1 := <-c
	f1()
}
