package walminio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/minio/minio-go/v7"
)

type MinWAL struct {
	client *minio.Client
	bucket string
	prefix string
	length uint64
	mu     sync.Mutex
}

func NewWAL(c *minio.Client, bucketName, prefix string) *MinWAL {
	return &MinWAL{
		client: c,
		bucket: bucketName,
		prefix: prefix,
		length: 0,
	}
}

func (w *MinWAL) getObjectKey(offset uint64) string {
	return w.prefix + "/" + fmt.Sprintf("%020d", offset)
}

func (w *MinWAL) getOffsetFromKey(key string) (uint64, error) {
	str := key[len(w.prefix)+1:]
	return strconv.ParseUint(str, 10, 64)
}

func checkSum(buff *bytes.Buffer) [32]byte {
	return sha256.Sum256(buff.Bytes())
}

// total trailer size is 36 bytes in which checksum is only 32 bytes and rest 4 bytes are mysterious 
// in most database implementations these 4 bytes are usually Record Length or TimeStamp.
func validateSum(data []byte) bool {
	var storedCheckSum [32]byte
	copy(storedCheckSum[:], data[len(data)-32:]) //adding : converts the fixed size array into slice so copy can write into it
	recordData := data[:len(data)-36]
	return storedCheckSum == checkSum(bytes.NewBuffer(recordData))
}

const (
	offSet = 8
	checkSumSize = 32
)
// this function takes raw data in bytes and wraps it in protective Envelope(header and trailer) and returns final byte slice ready for the disk
func prepareBody(offset uint64, data []byte) ([]byte, error) {
	// we are using 8 bytes for offset, len(data) bytes for data and 32 bytes for checksum
	buffLen := offSet + len(data)+ checkSumSize
	buff := make([]byte, buffLen)
	
	// write the offset(header) directly
	
	if err := binary.Write(buff, binary.BigEndian, offset); err != nil {
		return nil, err
	}
	if _, err := buff.Write(data); err != nil {
		return nil, err
	}
	
	// calculate checksum
	checkSum := checkSum(buff)
	_, err := buff.Write(checkSum[:])
	return buff.Bytes(), err
}

//main contracts start here like Append, Read and LastRecord
func (w *MinWAL) Append(ctx context.Context, data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	nextOffSet := w.length + 1
	
	buff, err ;= prepprepareBody(nexnextOffSet, data)
}
