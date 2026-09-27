package protocol

import (
	"bytes"
	"io"
	"testing"
)

func TestWriterReaderPrimitives(t *testing.T) {
	w := NewWriter()

	// Write primitive types
	w.WriteInt8(42)
	w.WriteInt8(-12)
	w.WriteInt16(1337)
	w.WriteInt16(-500)
	w.WriteInt32(100000)
	w.WriteInt32(-200000)
	w.WriteInt64(9876543210123)
	w.WriteInt64(-9876543210123)
	w.WriteString("kafka-unwedge-test")
	w.WriteString("")

	validStr := "instance-1"
	w.WriteNullableString(&validStr)
	w.WriteNullableString(nil)

	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	w.WriteBytes(rawBytes)
	w.WriteBytes(nil)

	w.WriteArrayLen(3)
	w.WriteRaw([]byte{0x01, 0x02})

	data := w.Bytes()
	r := NewReader(data)

	// Read and verify int8
	v8, err := r.ReadInt8()
	if err != nil || v8 != 42 {
		t.Fatalf("ReadInt8 failed: got %d, err %v", v8, err)
	}
	v8Neg, err := r.ReadInt8()
	if err != nil || v8Neg != -12 {
		t.Fatalf("ReadInt8 negative failed: got %d, err %v", v8Neg, err)
	}

	// Read and verify int16
	v16, err := r.ReadInt16()
	if err != nil || v16 != 1337 {
		t.Fatalf("ReadInt16 failed: got %d, err %v", v16, err)
	}
	v16Neg, err := r.ReadInt16()
	if err != nil || v16Neg != -500 {
		t.Fatalf("ReadInt16 negative failed: got %d, err %v", v16Neg, err)
	}

	// Read and verify int32
	v32, err := r.ReadInt32()
	if err != nil || v32 != 100000 {
		t.Fatalf("ReadInt32 failed: got %d, err %v", v32, err)
	}
	v32Neg, err := r.ReadInt32()
	if err != nil || v32Neg != -200000 {
		t.Fatalf("ReadInt32 negative failed: got %d, err %v", v32Neg, err)
	}

	// Read and verify int64
	v64, err := r.ReadInt64()
	if err != nil || v64 != 9876543210123 {
		t.Fatalf("ReadInt64 failed: got %d, err %v", v64, err)
	}
	v64Neg, err := r.ReadInt64()
	if err != nil || v64Neg != -9876543210123 {
		t.Fatalf("ReadInt64 negative failed: got %d, err %v", v64Neg, err)
	}

	// Read and verify strings
	str, err := r.ReadString()
	if err != nil || str != "kafka-unwedge-test" {
		t.Fatalf("ReadString failed: got '%s', err %v", str, err)
	}
	emptyStr, err := r.ReadString()
	if err != nil || emptyStr != "" {
		t.Fatalf("ReadString empty failed: got '%s', err %v", emptyStr, err)
	}

	// Read and verify nullable strings
	nullStrVal, err := r.ReadNullableString()
	if err != nil || nullStrVal == nil || *nullStrVal != "instance-1" {
		t.Fatalf("ReadNullableString valid failed: got %v, err %v", nullStrVal, err)
	}
	nullStrNil, err := r.ReadNullableString()
	if err != nil || nullStrNil != nil {
		t.Fatalf("ReadNullableString nil failed: got %v, err %v", nullStrNil, err)
	}

	// Read and verify bytes
	bVal, err := r.ReadBytes()
	if err != nil || !bytes.Equal(bVal, rawBytes) {
		t.Fatalf("ReadBytes valid failed: got %v, err %v", bVal, err)
	}
	bNil, err := r.ReadBytes()
	if err != nil || bNil != nil {
		t.Fatalf("ReadBytes nil failed: got %v, err %v", bNil, err)
	}

	// Read array length
	arrLen, err := r.ReadArrayLen()
	if err != nil || arrLen != 3 {
		t.Fatalf("ReadArrayLen failed: got %d, err %v", arrLen, err)
	}

	if r.Remaining() != 2 {
		t.Fatalf("Remaining mismatch: expected 2, got %d", r.Remaining())
	}
}

func TestReaderTruncation(t *testing.T) {
	// Truncated buffer for Int32
	r := NewReader([]byte{0x01, 0x02})
	_, err := r.ReadInt32()
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("Expected ErrUnexpectedEOF, got %v", err)
	}

	// Truncated string
	w := NewWriter()
	w.WriteInt16(10) // length 10
	w.WriteRaw([]byte("short"))
	r2 := NewReader(w.Bytes())
	_, err = r2.ReadString()
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("Expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestErrorCodeMappings(t *testing.T) {
	cases := map[int16]string{
		ErrNone:                      "NONE",
		ErrOffsetOutOfRange:          "OFFSET_OUT_OF_RANGE",
		ErrUnknownTopicOrPartition:   "UNKNOWN_TOPIC_OR_PARTITION",
		ErrNotLeaderForPartition:     "NOT_LEADER_FOR_PARTITION",
		ErrCoordinatorNotAvailable:   "COORDINATOR_NOT_AVAILABLE",
		ErrNotCoordinator:            "NOT_COORDINATOR",
		ErrIllegalGeneration:         "ILLEGAL_GENERATION",
		ErrUnknownMemberId:           "UNKNOWN_MEMBER_ID",
		ErrRebalanceInProgress:       "REBALANCE_IN_PROGRESS",
		ErrFencedInstanceId:          "FENCED_INSTANCE_ID",
		9999:                         "UNKNOWN_ERROR_9999",
	}

	for code, expected := range cases {
		actual := ErrorCodeText(code)
		if actual != expected {
			t.Errorf("ErrorCodeText(%d) = %s, expected %s", code, actual, expected)
		}
	}
}
