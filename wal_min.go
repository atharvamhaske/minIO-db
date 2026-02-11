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
	offSetSize = 8
	checkSumSize = 32
)
// this function takes raw data in bytes and wraps it in protective Envelope(header and trailer) and returns final byte slice ready for the disk
func prepareBody(offset uint64, data []byte) []byte {
	// we are using 8 bytes for offset, len(data) bytes for data and 32 bytes for checksum
	buffLen := offSetSize + len(data)+ checkSumSize
	buff := make([]byte, buffLen)
	
	// write the offset(header) directly
	binary.BigEndian.PutUint64(buff[:offSetSize], offset)
	copy(buff[offSetSize:], data)
	
	// calculate CheckSum for hash over Header + Body (everything before the checksum slot)
	payloadToChek := buff[:offSetSize+len(data)]
	sum := checkSum(payloadToChek)
	
	// write the checkSum (trailer) i.e copy the result into last 32 bytes of our buffer.
	copy(buff[len(buff)- checkSumSize:], sum[:])
	return buff
}

//main contracts start here like Append, Read and LastRecord
func (w *MinWAL) Append(ctx context.Context, data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	nextOffSet := w.length + 1
	
	buff, err ;= prepprepareBody(nexnextOffSet, data)
}
