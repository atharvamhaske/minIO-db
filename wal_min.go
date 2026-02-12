package walminio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
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
	offSetSize   = 8
	checkSumSize = 32
)

// this function takes raw data in bytes and wraps it in protective Envelope(header and trailer) and returns final byte slice ready for the disk
func prepareBody(offset uint64, data []byte) []byte {
	// we are using 8 bytes for offset, len(data) bytes for data and 32 bytes for checksum
	buffLen := offSetSize + len(data) + checkSumSize
	buff := make([]byte, buffLen)

	// write the offset(header) directly
	binary.BigEndian.PutUint64(buff[:offSetSize], offset)
	copy(buff[offSetSize:], data)

	// calculate CheckSum for hash over Header + Body (everything before the checksum slot)
	payloadToCheck := buff[:offSetSize+len(data)]
	sum := checkSum(bytes.NewBuffer(payloadToCheck))

	// write the checkSum (trailer) i.e copy the result into last 32 bytes of our buffer.
	copy(buff[len(buff)-checkSumSize:], sum[:])
	return buff
}

// main contracts start here like Append, Read and LastRecord
func (w *MinWAL) Append(ctx context.Context, data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	nextOffSet := w.length + 1

	buff := prepareBody(nextOffSet, data)

	input := &minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	}

	input.SetMatchETagExcept("*")

	_, err := w.client.PutObject(
		ctx,
		w.bucket,
		w.getObjectKey(nextOffSet),
		bytes.NewReader(buff),
		int64(len(buff)),
		*input)

	if err != nil {
		return 0, fmt.Errorf("failed to pute object on MinIO: %w", err)
	}
	w.length = nextOffSet
	return nextOffSet, nil
}

func (w *MinWAL) Read(ctx context.Context, offset uint64) (Record, error) {
	key := w.getObjectKey(offset)
	input := minio.GetObjectOptions{}

	object, err := w.client.GetObject(ctx, w.bucket, key, input)
	if err != nil {
		return Record{}, fmt.Errorf("failed to start get objects: %w", err)
	}
	defer object.Close()

	// read all bytes into memory
	data, err := io.ReadAll(object)
	if err != nil {
		// this is where we can usually get "Key Not Found" errors
		return Record{}, fmt.Errorf("failed to read object body from MinIO")
	}
	// validate length
	if len(data) < 40 {
		return Record{}, fmt.Errorf("invalid data, data is too short")
	}
	
	storedOffset := binary.BigEndian.Uint64(data[:8])
	
	if storedOffset != offset {
		return Record{}, fmt.Errorf("offset mismatch: expected %d, got %d", offset, storedOffset)
	}
	
	if !validateSum(data) {
		return Record{}, fmt.Errorf("checksum mismatch")
	}
	
	return Record{
		Offset: storedOffset,
		Data: data[8 : len(data)-32],
	}, nil
}

func (w *MinWAL) LastRecord(ctx context.Context) (Record, error) {
	input := minio.ListObjectsOptions{
		Prefix: w.prefix + "/",
		Recursive: true,
	}
	
	// MinIO's ListObjects returns a channel that automatically handles pagination
	objectCh := w.client.ListObjects(ctx, w.bucket, input)
	
	var maxOffset uint64 = 0
	found := false
	
	for obj := range objectCh {
		if obj.Err != nil {
			return Record{}, fmt.Errorf("failed to list objects from MinIO")
		}
		// parse the offset from the key (e.g., "wal/xxxxxxxxxx")
		offset, err := w.getOffsetFromKey(obj.Key)
		if err != nil {
			return Record{}, fmt.Errorf("failed to parse offset from key")
		}
		
		if offset > maxOffset {
			maxOffset = offset
		}
		found = true
	}
	if !found {
		return Record{}, fmt.Errorf("WAL is empty")
	}
	w.length = maxOffset
	
	return w.Read(ctx, maxOffset)
}