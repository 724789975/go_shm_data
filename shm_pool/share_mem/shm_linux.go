package share_mem

/*
#include <sys/shm.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type ShareMem[T any] struct {
	p uintptr
}

func CreateShareMem[T any](pathname string, gendid int8, size int) (*ShareMem[T], error) {
	f, err1 := os.OpenFile(pathname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err1 != nil{
		fmt.Println("[error]", err1.Error())
		return nil, err1
	}
	f.Close()
	pn := C.CString(pathname)
	fmt.Println(pn)
	fmt.Println(gendid)
	key := C.ftok(pn, (C.int)(gendid))
	fmt.Println(key)
	C.free(unsafe.Pointer(pn))
	var t T
	shmid, _, err := syscall.Syscall(syscall.SYS_SHMGET, uintptr(key), unsafe.Sizeof(t) * uintptr(size)+ uintptr(8), 01000|0640)//01000|0640
	if err != 0 {
		fmt.Println("[error]222333", key, err.Error())
		return nil, fmt.Errorf(err.Error())
	}
	s := new(ShareMem[T])
	s.p, _, err = syscall.Syscall(syscall.SYS_SHMAT, shmid, 0, 0)
	if err != 0 {
		fmt.Println("[error]444555", err.Error())
		return nil, fmt.Errorf(err.Error())
	}
	return s, nil
}

func (s *ShareMem[T]) Close() error {
	_, _, err := syscall.Syscall(syscall.SYS_SHMDT, s.p, 0, 0)
	if len(err.Error()) > 0 {
		return fmt.Errorf(err.Error())
	}
	return nil
}

func (s *ShareMem[T]) GetPtr() *T {
	return (*T)(unsafe.Pointer(s.p))
}