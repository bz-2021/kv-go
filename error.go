package kv_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("the data directory maybe corrupted")
)
