package index

import (
	"KV-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续接入其他数据结构，只需实现这个接口
type Indexer interface {
	// Put 向索引中插入Key-Value
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据Key取出Value
	Get(key []byte) *data.LogRecordPos

	// Delete 根据Key删除对应的Value
	Delete(key []byte) bool

	// Size 索引中数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewIndexer 根据类型初始化初始化
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 定义一个通用的索引迭代器的接口
type Iterator interface {
	Rewind()                   // 重新回到迭代器的起点
	Seek(key []byte)           // 根据传入的 Key 查找到第一个大于等于的目标 Key，根据这个 Key 开始遍历
	Next()                     // 跳转到下一个 Key
	Valid() bool               // 是否有效，即是否已经遍历完了所有的 Key，用于退出遍历
	Key() []byte               // 当前遍历位置的 Key 数据
	Value() *data.LogRecordPos // 当前遍历位置的 Value 数据
	Close()                    // 关闭迭代器，释放相应的资源
}
