package data

import (
	"KV-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 根据 path 和 id 生成完整的文件名称
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据 offset 从数据文件中读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 拿到当前文件的大小
	size, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 长度已经超过了文件的长度，则只需读取到文件的末尾即可
	var headerBytes int64 = maxLongRecordHeaderSize
	if offset+maxLongRecordHeaderSize > size {
		headerBytes = size - offset
	}

	// 读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 对 Header 信息进行解码
	header, headerSize := decodeLogRecordHeader(headerBuf)

	// 读取到了文件的末尾，直接返回EOF
	if header == nil {
		return nil, 0, io.EOF
	}

	// 如果读取到的文件的校验值为0，也认为读到了末尾，直接进行返回
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	// 定义 logRecord 结构体
	logRecord := &LogRecord{Type: header.recordType}
	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		KVBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// 解出 key 和 value
		// Go 语言中的切片语法，将字节数组中的前 keySize 个元素和剩余的元素取出来
		logRecord.Key = KVBuf[:keySize]
		logRecord.Value = KVBuf[keySize:]
	}

	// 校验对应的数据的 crc 是否正确
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
