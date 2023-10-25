package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常编码一条数据
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// LogRecord 中 Value 为空的情况
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	header1, size1 := decodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, header1)
	assert.Equal(t, size1, int64(7))
	assert.Equal(t, header1.crc, uint32(2532332136))
	assert.Equal(t, LogRecordNormal, header1.recordType)
	assert.Equal(t, header1.keySize, uint32(4))
	assert.Equal(t, header1.valueSize, uint32(10))

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	header2, size2 := decodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, header2)
	assert.Equal(t, size2, int64(7))
	assert.Equal(t, header2.crc, uint32(240712713))
	assert.Equal(t, LogRecordNormal, header2.recordType)
	assert.Equal(t, header2.keySize, uint32(4))
	assert.Equal(t, header2.valueSize, uint32(0))

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	header3, size3 := decodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, header3)
	assert.Equal(t, size3, int64(7))
	assert.Equal(t, header3.crc, uint32(290887979))
	assert.Equal(t, LogRecordDeleted, header3.recordType)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, crc1, uint32(2532332136))

	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, crc2, uint32(240712713))
}
