package kv_go

import "os"

type Options struct {
	DirPath      string      // 数据目录
	DataFileSize int64       // 数据文件的大小
	SyncWrites   bool        // 每次写数据是否持久化
	IndexType    IndexerType // 索引类型
}

type IteratorOptions struct {
	// 遍历前缀为指定的 key，默认为空
	Prefix []byte
	// 是否反向遍历，默认为 false
	Reverse bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
