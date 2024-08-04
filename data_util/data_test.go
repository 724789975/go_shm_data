package data_util_test

import (
	"fmt"
	"goshm/data_util"
	"goshm/shm_pool/pool_interface"
	"net/http"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

type TestData2 struct {
	ID   int
	Name [10]byte
	Age  int
	Sex  int
}

type TestData struct {
	ID   int
	Name [10]byte
	Age  int

	T2 map[int]*TestData2
}

type ShmTestData2 struct {
	ID   int
	Name [10]byte
	Age  int
	Sex  int
}

type ShmTestData struct {
	ID   int
	Name [10]byte
	Age  int
}

type ShmTestDatas struct {
	Pre  data_util.DataPreparePtr[int, ShmTestData]
	Pre2 map[int]*data_util.DataPreparePtr[int, ShmTestData2]
	Rel  data_util.DataLandingPtr[int, ShmTestData]
	Rel2 map[int]*data_util.DataLandingPtr[int, ShmTestData2]
}

var dm *data_util.DataMgr[int, TestData, ShmTestDatas]

var shm_t1 *pool_interface.ShmPool[data_util.PoolData[int, ShmTestData]]
var shm_t1b *pool_interface.ShmPool[data_util.PoolData[int, ShmTestData]]
var shm_t2 *pool_interface.ShmPool[data_util.PoolData[int, ShmTestData2]]
var shm_t2b *pool_interface.ShmPool[data_util.PoolData[int, ShmTestData2]]

var testDataLen int = 10240

func TestMain(m *testing.M) {
	//测试环境初始化开始

	debug.SetGCPercent(1000)

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		p := pprof.Lookup("goroutine")
		p.WriteTo(w, 1)
	}

	go func() {
		debugSvr := http.NewServeMux()
		debugSvr.HandleFunc("/", handler)
		_ = http.ListenAndServe("0.0.0.0:6060", debugSvr)
	}()

	var err error
	shm_t1, err = pool_interface.CreateShmPool[data_util.PoolData[int, ShmTestData]]("test_data1.db", 10, testDataLen)
	if err != nil {
		panic(err)
	}
	shm_t1b, err = pool_interface.CreateShmPool[data_util.PoolData[int, ShmTestData]]("test_data1b.db", 11, testDataLen)
	if err != nil {
		panic(err)
	}
	shm_t2, err = pool_interface.CreateShmPool[data_util.PoolData[int, ShmTestData2]]("test_data2.db", 12, testDataLen*10)
	if err != nil {
		panic(err)
	}
	shm_t2b, err = pool_interface.CreateShmPool[data_util.PoolData[int, ShmTestData2]]("test_data2b.db", 13, testDataLen*10)
	if err != nil {
		panic(err)
	}

	load_data_func := func(id int) (TestData, error) {
		// fmt.Println("load data:", id)
		td := TestData{
			ID:   id,
			Name: [10]byte{'t', 'e', 's', 't', '1', '1'},
			Age:  20,
			T2:   make(map[int]*TestData2),
		}
		td.T2[id<<16+1] = &TestData2{
			ID:   1,
			Name: [10]byte{'t', 'e', 's', 't', '2', '1'},
			Age:  20,
		}
		td.T2[id<<16+2] = &TestData2{
			ID:   2,
			Name: [10]byte{'t', 'e', 's', 't', '2', '2'},
			Age:  20,
		}
		return td, nil
	}
	fetch_shm_func := func() (ShmTestDatas, error) {
		stdata := ShmTestDatas{
			Pre:  data_util.DataPreparePtr[int, ShmTestData]{ShmPtr: shm_t1.Get()},
			Pre2: make(map[int]*data_util.DataPreparePtr[int, ShmTestData2]),
			Rel:  data_util.DataLandingPtr[int, ShmTestData]{Dirty: true, ShmPtr: shm_t1b.Get()},
			Rel2: make(map[int]*data_util.DataLandingPtr[int, ShmTestData2]),
		}
		if stdata.Pre.ShmPtr == nil || stdata.Rel.ShmPtr == nil {
			// panic(fmt.Sprintf("shm pool get nil cache size %d", dm.GetCacheSize()))
			return ShmTestDatas{}, fmt.Errorf("shm pool get nil")
		}
		stdata.Pre.ShmPtr.OpType = data_util.DataUnitOpTypeNone
		stdata.Pre.ShmPtr.Data = ShmTestData{
			ID:   0,
			Name: [10]byte{},
			Age:  0,
		}
		stdata.Pre.ShmPtr.Recorded = false
		stdata.Pre.ShmPtr.UesShm = false
		stdata.Rel.ShmPtr.OpType = data_util.DataUnitOpTypeNone
		stdata.Rel.ShmPtr.Data = ShmTestData{
			ID:   0,
			Name: [10]byte{},
			Age:  0,
		}
		stdata.Rel.ShmPtr.Recorded = false
		stdata.Rel.ShmPtr.UesShm = false
		stdata.Rel.Dirty = false

		return stdata, nil
	}
	release_shm_func := func(shmd *ShmTestDatas) {
		// fmt.Println("release data")

		for _, v := range shmd.Rel2 {
			v.ShmPtr.UesShm = false
			shm_t2b.Put(v.ShmPtr)
		}
		clear(shmd.Rel2)
		shmd.Rel.ShmPtr.UesShm = false
		shm_t1b.Put(shmd.Rel.ShmPtr)
		shmd.Rel.ShmPtr = nil

		for _, v := range shmd.Pre2 {
			v.ShmPtr.UesShm = false
			shm_t2.Put(v.ShmPtr)
		}
		clear(shmd.Pre2)
		shmd.Pre.ShmPtr.UesShm = false
		shm_t1.Put(shmd.Pre.ShmPtr)
		shmd.Pre.ShmPtr = nil
	}
	data_del_func := func(du *data_util.DataUnit[TestData, ShmTestDatas]) {
		du.ShmData.Pre.ShmPtr.OpType = data_util.DataUnitOpTypeDel
		for _, v := range du.ShmData.Pre2 {
			v.ShmPtr.OpType = data_util.DataUnitOpTypeDel
		}
		du.ShmData.Rel.ShmPtr.OpType = data_util.DataUnitOpTypeDel
		for _, v := range du.ShmData.Rel2 {
			v.ShmPtr.OpType = data_util.DataUnitOpTypeDel
		}
		du.Landing(du)
		du.ShmData.Pre.ShmPtr.UesShm = false
		shm_t1.Put(du.ShmData.Pre.ShmPtr)
		du.ShmData.Pre.ShmPtr = nil
		for _, v := range du.ShmData.Pre2 {
			v.ShmPtr.UesShm = false
			shm_t2.Put(v.ShmPtr)
			v.ShmPtr = nil
		}
		du.ShmData.Rel.ShmPtr.UesShm = false
		shm_t1b.Put(du.ShmData.Rel.ShmPtr)
		du.ShmData.Rel.ShmPtr = nil
		for _, v := range du.ShmData.Rel2 {
			v.ShmPtr.UesShm = false
			shm_t2b.Put(v.ShmPtr)
			v.ShmPtr = nil
		}
	}
	data_load_func := func(td *TestData, shmd *ShmTestDatas) {
		shmd.Pre.ShmPtr.Key = td.ID
		shmd.Pre.ShmPtr.OpType = data_util.DataUnitOpTypeSet
		shmd.Pre.ShmPtr.Data = ShmTestData{
			ID:   td.ID,
			Name: td.Name,
			Age:  td.Age,
		}
		for k, v := range td.T2 {
			shmd.Pre2[k] = &data_util.DataPreparePtr[int, ShmTestData2]{ShmPtr: shm_t2.Get()}
			shmd.Pre2[k].ShmPtr.Key = k
			shmd.Pre2[k].ShmPtr.Data = ShmTestData2{
				ID:   v.ID,
				Name: v.Name,
				Age:  v.Age,
			}
			shmd.Pre2[k].ShmPtr.OpType = data_util.DataUnitOpTypeSet
			shmd.Pre2[k].ShmPtr.UesShm = true
			shmd.Pre2[k].ShmPtr.Recorded = true
		}
		shmd.Pre.ShmPtr.Recorded = true
		shmd.Pre.ShmPtr.UesShm = true

		shmd.Rel.ShmPtr.Key = td.ID
		shmd.Rel.ShmPtr.OpType = data_util.DataUnitOpTypeSet
		shmd.Rel.ShmPtr.Data = ShmTestData{
			ID:   td.ID,
			Name: td.Name,
			Age:  td.Age,
		}
		shmd.Rel.Dirty = false
		for k, v := range td.T2 {
			shmd.Rel2[k] = &data_util.DataLandingPtr[int, ShmTestData2]{Dirty: false, ShmPtr: shm_t2b.Get()}
			shmd.Rel2[k].ShmPtr.Key = k
			shmd.Rel2[k].ShmPtr.Data = ShmTestData2{
				ID:   v.ID,
				Name: v.Name,
				Age:  v.Age,
			}
			shmd.Rel2[k].ShmPtr.OpType = data_util.DataUnitOpTypeSet
			shmd.Rel2[k].ShmPtr.UesShm = true
			shmd.Rel2[k].ShmPtr.Recorded = true
		}
		shmd.Rel.ShmPtr.Recorded = true
		shmd.Rel.ShmPtr.UesShm = true
	}
	// data_new_func
	mk := make(map[int]bool)
	data_new_func := func(k int, td TestData) error {
		if _, ok := mk[k]; ok {
			return data_util.NewDataErr(data_util.ErrCodeDataAlreadyExists)
		}
		return nil
	}
	data_landing_func := func(du *data_util.DataUnit[TestData, ShmTestDatas]) {
		shmd := &du.ShmData
		if shmd.Rel.ShmPtr == nil {
			fmt.Println("rel shm is nil, key:", du.Data.ID)
			return
		}
		if shmd.Rel.Dirty {
			// fmt.Println("commit data")
			switch shmd.Rel.ShmPtr.OpType {
			case data_util.DataUnitOpTypeSet:
				// fmt.Printf("set data:%d\n", shmd.Rel.ShmPtr.Data.ID)
				// TODO: set data
				du.DBLanding(func() {})
			case data_util.DataUnitOpTypeDel:
				// fmt.Printf("del data:%d\n", shmd.Rel.ShmPtr.Data.ID)
				// TODO: del data
				du.DBLanding(func() {})
			default:
				panic("unknown op type")
			}
			shmd.Rel.Dirty = false
		}

		waitDel := make([]int, 0)
		for _, v := range shmd.Rel2 {
			if v.Dirty {
				switch v.ShmPtr.OpType {
				case data_util.DataUnitOpTypeSet:
					// fmt.Printf("set data2:%d\n", v.ShmPtr.Data.ID)
					// TODO: set data2
					du.DBLanding(func() {})
				case data_util.DataUnitOpTypeDel:
					// fmt.Printf("del data2:%d\n", v.ShmPtr.Data.ID)
					// TODO: del data2
					du.DBLanding(func() {})
					waitDel = append(waitDel, v.ShmPtr.Data.ID)
				default:
					panic("unknown op type")
				}
				v.Dirty = false
			}
		}
		for _, v := range waitDel {
			delete(shmd.Pre2, v)
			delete(shmd.Rel2, v)
		}
	}

	dm = data_util.CreateDataMgr(testDataLen/2,
		load_data_func,
		func() {
			mapPre1 := make(map[int]*data_util.PoolData[int, ShmTestData])
			pa := make([]*data_util.PoolData[int, ShmTestData], 0)
			for i := 0; i < testDataLen; i++ {
				p := shm_t1.Get()
				if p.UesShm {
					mapPre1[p.Key] = p
				} else {
					pa = append(pa, p)
				}
			}
			for _, v := range pa {
				v.UesShm = false
				shm_t1.Put(v)
			}
			// fmt.Printf("test data init load111111111 mapPre1 size %d, shm_t1 %d\n", len(mapPre1), shm_t1.GetLeftSize())
			mapRel1 := make(map[int]*data_util.PoolData[int, ShmTestData])
			pa2 := make([]*data_util.PoolData[int, ShmTestData], 0)
			for i := 0; i < testDataLen; i++ {
				p := shm_t1b.Get()
				if p.UesShm {
					mapRel1[p.Key] = p
				} else {
					pa2 = append(pa2, p)
				}
			}

			for _, v := range pa2 {
				v.UesShm = false
				shm_t1b.Put(v)
			}

			mapPre2 := make(map[int]*data_util.PoolData[int, ShmTestData2])
			pa3 := make([]*data_util.PoolData[int, ShmTestData2], 0)
			for i := 0; i < testDataLen*10; i++ {
				p := shm_t2.Get()
				if p.UesShm {
					mapPre2[p.Key] = p
				} else {
					pa3 = append(pa3, p)
				}
			}

			for _, v := range pa3 {
				v.UesShm = false
				shm_t2.Put(v)
			}

			mapRel2 := make(map[int]*data_util.PoolData[int, ShmTestData2])
			pa4 := make([]*data_util.PoolData[int, ShmTestData2], 0)
			for i := 0; i < testDataLen*10; i++ {
				p := shm_t2b.Get()
				if p.UesShm {
					mapRel2[p.Key] = p
				} else {
					pa4 = append(pa4, p)
				}
			}

			for _, v := range pa4 {
				v.UesShm = false
				shm_t2b.Put(v)
			}

			if len(mapPre1) < len(mapRel1) {
				panic("pre1 shm pool is less than rel1 shm pool")
			}

			for k, v := range mapRel1 {
				if _, ok := mapPre1[k]; !ok {
					panic("rel shm pool has key not in pre shm pool")
				}

				if !v.Recorded && !mapPre1[k].Recorded {
					panic("shm pool has not recorded")
				}
				if !v.Recorded {
					v.Data = mapPre1[k].Data
					v.OpType = mapPre1[k].OpType
					v.Recorded = true
				}
			}

			if len(mapPre2) < len(mapRel2) {
				panic("pre2 shm pool is less than rel2 shm pool")
			}

			for k, v := range mapRel2 {
				if _, ok := mapPre2[k]; !ok {
					panic("rel2 shm pool has key not in pre2 shm pool")
				}

				if !v.Recorded && !mapPre2[k].Recorded {
					panic("shm pool has not recorded")
				}
				if !v.Recorded {
					v.Data = mapPre2[k].Data
					v.OpType = mapPre2[k].OpType
					v.Recorded = true
				}
			}

			fmt.Printf("mapPre1 len %d\n", len(mapRel1))
			for _, v := range mapPre1 {
				v.UesShm = false
				shm_t1.Put(v)
			}
			clear(mapPre1)

			for _, v := range mapPre2 {
				v.UesShm = false
				shm_t2.Put(v)
			}
			clear(mapPre2)

			mapData2 := make(map[int][]*data_util.PoolData[int, ShmTestData2])
			for _, v := range mapRel2 {
				if _, ok := mapData2[v.Key>>16]; !ok {
					mapData2[v.Key>>16] = make([]*data_util.PoolData[int, ShmTestData2], 0)
				}
				mapData2[v.Key>>16] = append(mapData2[v.Key>>16], v)
			}

			//恢复数据
			fmt.Printf("test data init cache size %d, shm left size %d, %d\n", dm.GetCacheSize(), shm_t1.GetLeftSize(), shm_t1b.GetLeftSize())

			fmt.Printf("test data init mapRel1 len %d\n", len(mapRel1))
			for k, v := range mapRel1 {
				td := TestData{
					ID:   v.Key,
					Name: v.Data.Name,
					Age:  v.Data.Age,
					T2:   make(map[int]*TestData2),
				}
				shmtd := ShmTestDatas{
					Pre:  data_util.DataPreparePtr[int, ShmTestData]{ShmPtr: shm_t1.Get()},
					Pre2: make(map[int]*data_util.DataPreparePtr[int, ShmTestData2]),
					Rel:  data_util.DataLandingPtr[int, ShmTestData]{Dirty: false, ShmPtr: v},
					Rel2: make(map[int]*data_util.DataLandingPtr[int, ShmTestData2]),
				}
				*shmtd.Pre.ShmPtr = *v

				if arr, ok := mapData2[v.Key]; ok {
					for _, v2 := range arr {
						shmtd.Rel2[v2.Key] = &data_util.DataLandingPtr[int, ShmTestData2]{Dirty: false, ShmPtr: v2}
						shmtd.Pre2[v2.Key] = &data_util.DataPreparePtr[int, ShmTestData2]{ShmPtr: shm_t2.Get()}
						*shmtd.Pre2[v2.Key].ShmPtr = *v2
						delete(mapRel2, v2.Key)
					}
				}

				mk[k] = true
				dm.PushData(k, k, td, shmtd)
			}

			for _, v := range mapRel2 {
				fmt.Println("rel2 shm pool has key not in pre2 shm pool:", v.Key)
				v.UesShm = false
				shm_t2b.Put(v)
			}
			clear(mapRel2)

			fmt.Printf("test data init done cache size %d, shm left size %d, %d\n", dm.GetCacheSize(), shm_t1.GetLeftSize(), shm_t1b.GetLeftSize())
		},
		fetch_shm_func,
		release_shm_func,
		data_del_func,
		data_load_func,
		data_new_func,
		data_landing_func,
	)
	retCode := m.Run()

	fmt.Println("TestMain tear-down...")
	os.Exit(retCode)
}

func Test_Data_Test(t *testing.T) {

	ch := make(chan bool)
	// dm.NewData(1, 1, TestData{ID: 1, Name: [10]byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}, Age: 20, T2: make(map[int]TestData2)}, func(v *TestData, err error) {
	// 	ch <- true
	// })
	// <-ch

	dm.OpData(1, 1, func(id int, err error) {
		// fmt.Println("get data error:", err, id)
	},
		func(td *TestData, shm *ShmTestDatas) error {
			td.Age++

			/////////////////////以下是落地的代码/////////////////////
			if shm.Pre.ShmPtr == nil {
				return fmt.Errorf("shm pre is nil")
			}
			if shm.Pre.ShmPtr.OpType == data_util.DataUnitOpTypeDel {
				return fmt.Errorf("shm pre is del")
			}
			shm.Pre.ShmPtr.Recorded = false
			shm.Pre.ShmPtr.Data.Age = td.Age
			shm.Pre.ShmPtr.Recorded = true

			shm.Rel.ShmPtr.Recorded = false
			shm.Rel.ShmPtr.Data = shm.Pre.ShmPtr.Data
			shm.Rel.Dirty = true
			shm.Rel.ShmPtr.Recorded = true
			/////////////////////以上是落地的代码/////////////////////

			fmt.Println("get data:", td.ID, td.Age)
			ch <- true
			return nil
		},
	)
	<-ch

	fmt.Println("test data")

	dm.OpData(1, 1, func(id int, err error) {},
		func(td *TestData, shm *ShmTestDatas) error {
			td.T2[td.ID<<16+1] = &TestData2{
				ID:   td.ID<<16 + 1,
				Name: [10]byte{'t', 'e', 's', 't', '2', '1'},
				Age:  20,
			}

			/////////////////////以下是落地的代码/////////////////////
			if shm.Pre.ShmPtr == nil {
				return fmt.Errorf("shm pre is nil")
			}
			if shm.Pre.ShmPtr.OpType == data_util.DataUnitOpTypeDel {
				return fmt.Errorf("shm pre is del")
			}

			if _, ok := shm.Pre2[td.ID<<16+1]; !ok {
				shm.Pre2[td.ID<<16+1] = &data_util.DataPreparePtr[int, ShmTestData2]{ShmPtr: shm_t2.Get()}
				// shm.Pre2[td.ID<<16+1].ShmPtr.OpType = data_util.DataUnitOpTypeAdd
			}
			shm.Pre2[td.ID<<16+1].ShmPtr.UesShm = true
			shm.Pre2[td.ID<<16+1].ShmPtr.Set(td.ID<<16+1, ShmTestData2{
				ID:   td.ID<<16 + 1,
				Name: [10]byte{'t', 'e', 's', 't', '2', '1'},
				Age:  20,
			})

			if _, ok := shm.Rel2[td.ID<<16+1]; !ok {
				shm.Rel2[td.ID<<16+1] = &data_util.DataLandingPtr[int, ShmTestData2]{Dirty: true, ShmPtr: shm_t2b.Get()}
				// shm.Rel2[td.ID<<16+1].ShmPtr.OpType = data_util.DataUnitOpTypeAdd
			}
			shm.Rel2[td.ID<<16+1].ShmPtr.UesShm = true
			shm.Rel2[td.ID<<16+1].ShmPtr.Set(td.ID<<16+1, shm.Pre2[td.ID<<16+1].ShmPtr.Data)

			shm.Pre.ShmPtr.Recorded = false
			shm.Pre.ShmPtr.Data.Age = td.Age
			shm.Pre.ShmPtr.Recorded = true

			shm.Rel.ShmPtr.Recorded = false
			shm.Rel.ShmPtr.Data.Age = td.Age
			shm.Rel.Dirty = true
			shm.Rel.ShmPtr.Recorded = true
			/////////////////////以上是落地的代码/////////////////////

			ch <- true
			return nil
		},
	)

	fmt.Println("ch len:", len(ch))
	<-ch
	fmt.Println("test data 2222 cache size:", dm.GetCacheSize())

	wg := sync.WaitGroup{}
	for i := 1; i < 9500; i++ {
		wg.Add(1)
		func() {
			k := i
			ch1 := make(chan bool)
			dm.NewData(k, k, TestData{ID: k, Name: [10]byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}, Age: 20}, func(v *TestData, err error) {
				if err != nil {
					fmt.Println("new data error:", err)
					ch1 <- false
					return
				}
				ch1 <- true
			})
			<-ch1
			wg.Done()
		}()
	}

	fmt.Println("test data 3333 cache size:", dm.GetCacheSize(), shm_t1.GetLeftSize())
	time.Sleep(time.Second * 100)

	wg.Wait()

	for i := 0; i < 9030; i++ {
		wg.Add(1)
		k := i
		go func() {
			for j := 0; j < 10; j++ {
				ch2 := make(chan bool)
				go func() {
					<-ch2
				}()
				dm.OpData(k, k, func(id int, err error) {
					if err != nil {
						fmt.Println("get data error:", err, id)
						// if len(ch2) > 0{
						// 	panic("get data error")
						// }
						ch2 <- false
					}
				}, func(v *TestData, shm *ShmTestDatas) error {
					v.Age++
					// if len(ch2) > 0{
					// 	panic("get data error")
					// }
					// fmt.Println("get data error:", v.ID)
					ch2 <- true
					return nil
				})
				time.Sleep(time.Millisecond * 10)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	fmt.Println("test data 4444 cache size:", dm.GetCacheSize(), shm_t1.GetLeftSize())
	time.Sleep(time.Second * 100000)

}
