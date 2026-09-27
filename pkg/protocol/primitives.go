package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Common Kafka protocol error codes.
const (
	ErrNone                        int16 = 0
	ErrOffsetOutOfRange            int16 = 1
	ErrCorruptMessage              int16 = 2
	ErrUnknownTopicOrPartition     int16 = 3
	ErrInvalidFetchSize            int16 = 4
	ErrLeaderNotAvailable          int16 = 5
	ErrNotLeaderForPartition       int16 = 6
	ErrRequestTimedOut             int16 = 7
	ErrBrokerNotAvailable          int16 = 8
	ErrReplicaNotAvailable         int16 = 9
	ErrMessageTooLarge             int16 = 10
	ErrStaleControllerEpoch        int16 = 11
	ErrOffsetMetadataTooLarge      int16 = 12
	ErrNetworkException            int16 = 13
	ErrCoordinatorLoadInProgress   int16 = 14
	ErrCoordinatorNotAvailable     int16 = 15
	ErrNotCoordinator              int16 = 16
	ErrInvalidTopic                int16 = 17
	ErrRecordListTooLarge          int16 = 18
	ErrNotEnoughReplicas           int16 = 19
	ErrNotEnoughReplicasAfterAppend int16 = 20
	ErrInvalidRequiredAcks         int16 = 21
	ErrIllegalGeneration           int16 = 22
	ErrInconsistentGroupProtocol   int16 = 23
	ErrInvalidGroupId              int16 = 24
	ErrUnknownMemberId             int16 = 25
	ErrInvalidSessionTimeout       int16 = 26
	ErrRebalanceInProgress         int16 = 27
	ErrInvalidCommitOffsetSize     int16 = 28
	ErrTopicAuthorizationFailed    int16 = 29
	ErrGroupAuthorizationFailed    int16 = 30
	ErrClusterAuthorizationFailed  int16 = 31
	ErrFencedInstanceId            int16 = 82
)

// ErrorCodeText maps a numeric Kafka error code to its standard identifier.
func ErrorCodeText(code int16) string {
	switch code {
	case ErrNone:
		return "NONE"
	case ErrOffsetOutOfRange:
		return "OFFSET_OUT_OF_RANGE"
	case ErrCorruptMessage:
		return "CORRUPT_MESSAGE"
	case ErrUnknownTopicOrPartition:
		return "UNKNOWN_TOPIC_OR_PARTITION"
	case ErrInvalidFetchSize:
		return "INVALID_FETCH_SIZE"
	case ErrLeaderNotAvailable:
		return "LEADER_NOT_AVAILABLE"
	case ErrNotLeaderForPartition:
		return "NOT_LEADER_FOR_PARTITION"
	case ErrRequestTimedOut:
		return "REQUEST_TIMED_OUT"
	case ErrBrokerNotAvailable:
		return "BROKER_NOT_AVAILABLE"
	case ErrReplicaNotAvailable:
		return "REPLICA_NOT_AVAILABLE"
	case ErrMessageTooLarge:
		return "MESSAGE_TOO_LARGE"
	case ErrStaleControllerEpoch:
		return "STALE_CONTROLLER_EPOCH"
	case ErrOffsetMetadataTooLarge:
		return "OFFSET_METADATA_TOO_LARGE"
	case ErrNetworkException:
		return "NETWORK_EXCEPTION"
	case ErrCoordinatorLoadInProgress:
		return "COORDINATOR_LOAD_IN_PROGRESS"
	case ErrCoordinatorNotAvailable:
		return "COORDINATOR_NOT_AVAILABLE"
	case ErrNotCoordinator:
		return "NOT_COORDINATOR"
	case ErrInvalidTopic:
		return "INVALID_TOPIC"
	case ErrRecordListTooLarge:
		return "RECORD_LIST_TOO_LARGE"
	case ErrNotEnoughReplicas:
		return "NOT_ENOUGH_REPLICAS"
	case ErrNotEnoughReplicasAfterAppend:
		return "NOT_ENOUGH_REPLICAS_AFTER_APPEND"
	case ErrInvalidRequiredAcks:
		return "INVALID_REQUIRED_ACKS"
	case ErrIllegalGeneration:
		return "ILLEGAL_GENERATION"
	case ErrInconsistentGroupProtocol:
		return "INCONSISTENT_GROUP_PROTOCOL"
	case ErrInvalidGroupId:
		return "INVALID_GROUP_ID"
	case ErrUnknownMemberId:
		return "UNKNOWN_MEMBER_ID"
	case ErrInvalidSessionTimeout:
		return "INVALID_SESSION_TIMEOUT"
	case ErrRebalanceInProgress:
		return "REBALANCE_IN_PROGRESS"
	case ErrInvalidCommitOffsetSize:
		return "INVALID_COMMIT_OFFSET_SIZE"
	case ErrTopicAuthorizationFailed:
		return "TOPIC_AUTHORIZATION_FAILED"
	case ErrGroupAuthorizationFailed:
		return "GROUP_AUTHORIZATION_FAILED"
	case ErrClusterAuthorizationFailed:
		return "CLUSTER_AUTHORIZATION_FAILED"
	case ErrFencedInstanceId:
		return "FENCED_INSTANCE_ID"
	default:
		return fmt.Sprintf("UNKNOWN_ERROR_%d", code)
	}
}

// Writer encapsulates serialization of Kafka wire protocol primitives.
type Writer struct {
	buf []byte
}

// NewWriter creates an empty buffer with optional initial capacity.
func NewWriter() *Writer {
	return &Writer{buf: make([]byte, 0, 256)}
}

// WriteInt8 writes a 1-byte signed integer.
func (w *Writer) WriteInt8(v int8) {
	w.buf = append(w.buf, byte(v))
}

// WriteInt16 writes a 2-byte big-endian signed integer.
func (w *Writer) WriteInt16(v int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	w.buf = append(w.buf, b[:]...)
}

// WriteInt32 writes a 4-byte big-endian signed integer.
func (w *Writer) WriteInt32(v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	w.buf = append(w.buf, b[:]...)
}

// WriteInt64 writes an 8-byte big-endian signed integer.
func (w *Writer) WriteInt64(v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.buf = append(w.buf, b[:]...)
}

// WriteString writes a Kafka standard string: 2-byte length followed by UTF-8 bytes.
func (w *Writer) WriteString(s string) {
	w.WriteInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

// WriteNullableString writes a Kafka nullable string: -1 for null, or 2-byte length + UTF-8 bytes.
func (w *Writer) WriteNullableString(s *string) {
	if s == nil {
		w.WriteInt16(-1)
		return
	}
	w.WriteInt16(int16(len(*s)))
	w.buf = append(w.buf, *s...)
}

// WriteBytes writes a Kafka byte array: 4-byte length prefix followed by raw bytes.
func (w *Writer) WriteBytes(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(b)))
	w.buf = append(w.buf, b...)
}

// WriteArrayLen writes a 4-byte array count prefix.
func (w *Writer) WriteArrayLen(n int) {
	w.WriteInt32(int32(n))
}

// WriteRaw appends raw bytes to the buffer without any length prefix.
func (w *Writer) WriteRaw(b []byte) {
	w.buf = append(w.buf, b...)
}

// Bytes returns the written bytes.
func (w *Writer) Bytes() []byte {
	return w.buf
}

// Reader encapsulates deserialization of Kafka wire protocol primitives.
type Reader struct {
	buf []byte
	pos int
}

// NewReader initializes a reader over a byte slice.
func NewReader(b []byte) *Reader {
	return &Reader{buf: b, pos: 0}
}

// Remaining returns how many unread bytes are left.
func (r *Reader) Remaining() int {
	return len(r.buf) - r.pos
}

// ReadInt8 reads a 1-byte signed integer.
func (r *Reader) ReadInt8() (int8, error) {
	if r.Remaining() < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	v := int8(r.buf[r.pos])
	r.pos++
	return v, nil
}

// ReadInt16 reads a 2-byte big-endian signed integer.
func (r *Reader) ReadInt16() (int16, error) {
	if r.Remaining() < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	v := int16(binary.BigEndian.Uint16(r.buf[r.pos : r.pos+2]))
	r.pos += 2
	return v, nil
}

// ReadInt32 reads a 4-byte big-endian signed integer.
func (r *Reader) ReadInt32() (int32, error) {
	if r.Remaining() < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos : r.pos+4]))
	r.pos += 4
	return v, nil
}

// ReadInt64 reads an 8-byte big-endian signed integer.
func (r *Reader) ReadInt64() (int64, error) {
	if r.Remaining() < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.pos : r.pos+8]))
	r.pos += 8
	return v, nil
}

// ReadString reads a 2-byte length-prefixed string.
func (r *Reader) ReadString() (string, error) {
	l, err := r.ReadInt16()
	if err != nil {
		return "", err
	}
	if l < 0 {
		return "", errors.New("unexpected negative string length")
	}
	length := int(l)
	if r.Remaining() < length {
		return "", io.ErrUnexpectedEOF
	}
	str := string(r.buf[r.pos : r.pos+length])
	r.pos += length
	return str, nil
}

// ReadNullableString reads a 2-byte length-prefixed nullable string.
func (r *Reader) ReadNullableString() (*string, error) {
	l, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}
	length := int(l)
	if r.Remaining() < length {
		return nil, io.ErrUnexpectedEOF
	}
	str := string(r.buf[r.pos : r.pos+length])
	r.pos += length
	return &str, nil
}

// ReadBytes reads a 4-byte length-prefixed byte array.
func (r *Reader) ReadBytes() ([]byte, error) {
	l, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}
	length := int(l)
	if r.Remaining() < length {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, length)
	copy(data, r.buf[r.pos:r.pos+length])
	r.pos += length
	return data, nil
}

// ReadArrayLen reads a 4-byte array count prefix.
func (r *Reader) ReadArrayLen() (int32, error) {
	return r.ReadInt32()
}
