package fio

const DataFilePerm = 0644

// IOManager 抽象 IO 管理接口，可以接入不同的IO类型，目前支持标准文件IO
type IOManager interface {

	// Read 从文件给定位置读取数据
	Read([]byte, int64) (int, error)

	// Write 写入字节组到文件
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager， 目前只支持标准 FileIO
func NewIOManager(filename string) (IOManager, error) {
	return NewFileIOManager(filename)
}
