package data_util

import (
	"fmt"
)

const (
	ErrCodeSuccess = iota
	ErrCodeDeconstruct
	ErrCodeParamError
	ErrCodeDataNotFound
	ErrCodeDataAlreadyExists
	ErrCodeDataAlreadyDeleted
	ErrCodeLoadDataFailed
	ErrCodeNoEnoughShmMem
	ErrcodeShmMemIsNil
)

type DataErr struct {
	Errcode int
}

func (de *DataErr) Error() string {
	return fmt.Sprintf("DataErr: %d", de.Errcode)
}

func NewDataErr(err_code int) *DataErr {
	return &DataErr{
		Errcode: err_code,
	}
}