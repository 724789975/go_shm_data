package share_mem_test

import (
	"fmt"
	"os"
	"runtime/debug"
	"testing"

	"github.com/724789975/go_shm_data/shm_pool/pool_interface"
)

func TestMain(m *testing.M) {
	//测试环境初始化开始

	debug.SetGCPercent(1000)
	retCode := m.Run()

	fmt.Println("TestMain tear-down...")
	os.Exit(retCode)
}

// 测试用例 必须以Test开头
func TestAuction(t *testing.T) {

	// type Auction struct {
	// 	Id     int
	// 	Price  int
	// 	Status int
	// }

	// m, _ := share_mem.CreateShareMem[Auction]("test.txt", 0, 10)

	// p := m.GetPtr()
	// p.Id = 1
	// p.Price = 100
	// p.Status = 1234
	// m.Close()
}

func Test2(t *testing.T) {
	type Auction struct {
		Id     int
		Price  int
		Status int
	}

	m, err := pool_interface.CreateShmPool[Auction]("test.txt", 0, 10)
	if err != nil {
		t.Error(err)
		return
	}
	p := m.Get()
	p.Id = 1
	p.Price = 100
	p.Status = 1234
}
