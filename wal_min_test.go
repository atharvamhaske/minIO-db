package walminio

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"
)

func TestNewWALInitializesFields(t *testing.T) {
	w := NewWAL(nil, "test-bucket", "wal")

	if w == nil {
		t.Fatal("NewWAL returned nil")
	}
	if w.client != nil {
		t.Fatal("expected nil client")
	}
	if w.bucket != "test-bucket" {
		t.Fatalf("bucket mismatch: got %q", w.bucket)
	}
	if w.prefix != "wal" {
		t.Fatalf("prefix mismatch: got %q", w.prefix)
	}
	if w.length != 0 {
		t.Fatalf("length should start at 0, got %d", w.length)
	}
}

func TestGetObjectKeyZeroPaddedOffset(t *testing.T) {
	w := &MinWAL{prefix: "wal"}

	got := w.getObjectKey(42)
	want := "wal/00000000000000000042"

	if got != want {
		t.Fatalf("getObjectKey mismatch: got %q, want %q", got, want)
	}
}

func TestGetOffsetFromKey(t *testing.T) {
	w := &MinWAL{prefix: "wal"}

	t.Run("valid key", func(t *testing.T) {
		offset, err := w.getOffsetFromKey("wal/00000000000000000123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if offset != 123 {
			t.Fatalf("offset mismatch: got %d, want 123", offset)
		}
	})

	t.Run("invalid key", func(t *testing.T) {
		_, err := w.getOffsetFromKey("wal/not-a-number")
		if err == nil {
			t.Fatal("expected parse error for invalid offset")
		}
	})
}

func TestCheckSumDeterministic(t *testing.T) {
	data := []byte("hello wal")
	sum := checkSum(bytes.NewBuffer(data))
	want := sha256.Sum256(data)

	if sum != want {
		t.Fatal("checksum output mismatch")
	}
}

func TestPrepareBodyLayoutAndChecksum(t *testing.T) {
	offset := uint64(7)
	payload := []byte("entry")

	body := prepareBody(offset, payload)

	wantLen := offSetSize + len(payload) + checkSumSize
	if len(body) != wantLen {
		t.Fatalf("body length mismatch: got %d, want %d", len(body), wantLen)
	}

	gotOffset := binary.BigEndian.Uint64(body[:offSetSize])
	if gotOffset != offset {
		t.Fatalf("offset mismatch in body: got %d, want %d", gotOffset, offset)
	}

	gotPayload := body[offSetSize : offSetSize+len(payload)]
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", gotPayload, payload)
	}

	wantSum := sha256.Sum256(body[:offSetSize+len(payload)])
	gotSum := body[len(body)-checkSumSize:]
	if !bytes.Equal(gotSum, wantSum[:]) {
		t.Fatal("checksum mismatch in prepared body")
	}
}

func TestValidateSumWithPrepareBodyReturnsTrue(t *testing.T) {
	body := prepareBody(9, []byte("payload"))

	if !validateSum(body) {
		t.Fatal("expected validateSum to pass for prepareBody output")
	}
}

func TestValidateSumFailsWhenBodyIsTampered(t *testing.T) {
	body := prepareBody(11, []byte("payload"))
	body[offSetSize] ^= 0x01

	if validateSum(body) {
		t.Fatal("expected validateSum to fail for tampered payload")
	}
}
