package share_mem

import (
	"syscall"
	"unsafe"
)
 
type ShareMem[T any] struct {
	view uintptr
	mapHandle syscall.Handle
	fileHandle syscall.Handle
}

func CreateShareMem[T any](pathname string, gendid int8, size int) (*ShareMem[T], error) {
	fileHandle, err := syscall.CreateFile(
		syscall.StringToUTF16Ptr(pathname),
		syscall.GENERIC_READ | syscall.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}

	if fileHandle == syscall.InvalidHandle {
		return nil, syscall.EINVAL
	}
	var t T
	mapHandle, err := syscall.CreateFileMapping(
		fileHandle,
		nil,
		syscall.PAGE_READWRITE,
		0,
		uint32(unsafe.Sizeof(t) * uintptr(size)+ uintptr(8)), // 映射文件的大小
		nil,
	)
	if err != nil {
		return nil, err
	}

	if mapHandle == 0 {
		return nil, syscall.EINVAL
	}

	view, err := syscall.MapViewOfFile(
		mapHandle,
		syscall.FILE_MAP_WRITE,
		0,
		0,
		0,
	)

	if err != nil {
		return nil, err
	}

	s := new(ShareMem[T])
	s.view = view
	s.mapHandle = mapHandle
	s.fileHandle = fileHandle
	return s, nil
}

func (s *ShareMem[T]) Close() error {
	if s.view == 0 {
		return nil
	}

	if err := syscall.UnmapViewOfFile(s.view); err != nil {
		return err
	}

	if err := syscall.CloseHandle(s.mapHandle); err != nil {
		return err
	}

	if err := syscall.CloseHandle(s.fileHandle); err != nil {
		return err
	}

	return nil
}

func (s *ShareMem[T]) GetPtr() *T {
	return (*T)(unsafe.Pointer(s.view))
}