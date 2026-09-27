package protocol

import (
	"fmt"
	"net"
)

// API Key for FindCoordinator.
const ApiKeyFindCoordinator int16 = 10

// Coordinator types for FindCoordinator v1+.
const (
	CoordinatorTypeGroup int8 = 0
	CoordinatorTypeTxn   int8 = 1
)

// CoordinatorResponse represents the decoded result of FindCoordinator (v1).
type CoordinatorResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   *string
	NodeID         int32
	Host           string
	Port           int32
}

// Addr returns host:port formatted string for dialing.
func (c *CoordinatorResponse) Addr() string {
	return net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port))
}

// FindCoordinator executes FindCoordinator (API Key 10, Version 1) for a consumer group.
func FindCoordinator(conn *Connection, groupID string) (*CoordinatorResponse, error) {
	// Build FindCoordinator Request v1:
	// Key: string (group ID)
	// KeyType: int8 (0 = Group)
	w := NewWriter()
	w.WriteString(groupID)
	w.WriteInt8(CoordinatorTypeGroup)

	respBytes, err := conn.SendRequest(ApiKeyFindCoordinator, 1, w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("FindCoordinator request failed: %w", err)
	}

	// Parse FindCoordinator Response v1:
	// ThrottleTimeMs: int32
	// ErrorCode: int16
	// ErrorMessage: nullable string
	// NodeId: int32
	// Host: string
	// Port: int32
	r := NewReader(respBytes)

	throttleTimeMs, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read ThrottleTimeMs: %w", err)
	}

	errCode, err := r.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read ErrorCode: %w", err)
	}

	errMsg, err := r.ReadNullableString()
	if err != nil {
		return nil, fmt.Errorf("failed to read ErrorMessage: %w", err)
	}

	nodeID, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read NodeId: %w", err)
	}

	host, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("failed to read Host: %w", err)
	}

	port, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read Port: %w", err)
	}

	resp := &CoordinatorResponse{
		ThrottleTimeMs: throttleTimeMs,
		ErrorCode:      errCode,
		ErrorMessage:   errMsg,
		NodeID:         nodeID,
		Host:           host,
		Port:           port,
	}

	if resp.ErrorCode != ErrNone {
		errDetail := ErrorCodeText(resp.ErrorCode)
		if resp.ErrorMessage != nil && *resp.ErrorMessage != "" {
			errDetail = fmt.Sprintf("%s: %s", errDetail, *resp.ErrorMessage)
		}
		return resp, fmt.Errorf("coordinator error (code %d): %s", resp.ErrorCode, errDetail)
	}

	return resp, nil
}
