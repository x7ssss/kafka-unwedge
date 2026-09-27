package protocol

import (
	"fmt"
)

// API Key for LeaveGroup.
const ApiKeyLeaveGroup int16 = 13

// LeaveMemberIdentity specifies an identity to evict from a consumer group.
type LeaveMemberIdentity struct {
	MemberId        string
	GroupInstanceId *string
}

// LeaveMemberResponse details eviction results for an individual member.
type LeaveMemberResponse struct {
	MemberId        string
	GroupInstanceId *string
	ErrorCode       int16
}

// LeaveGroupResponse contains broker eviction status.
type LeaveGroupResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	Members        []LeaveMemberResponse
}

// LeaveGroup executes LeaveGroup (API Key 13, Version 3) to evict members or static instances.
func LeaveGroup(conn *Connection, groupID string, identities []LeaveMemberIdentity) (*LeaveGroupResponse, error) {
	w := NewWriter()
	w.WriteString(groupID)
	w.WriteArrayLen(len(identities))

	for _, id := range identities {
		w.WriteString(id.MemberId)
		w.WriteNullableString(id.GroupInstanceId)
	}

	respBytes, err := conn.SendRequest(ApiKeyLeaveGroup, 3, w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("LeaveGroup request failed: %w", err)
	}

	r := NewReader(respBytes)

	throttleTimeMs, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read ThrottleTimeMs: %w", err)
	}

	topErrCode, err := r.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read top-level ErrorCode: %w", err)
	}

	memberCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to read member response count: %w", err)
	}

	memberResponses := make([]LeaveMemberResponse, 0, memberCount)
	for i := 0; i < int(memberCount); i++ {
		mid, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read MemberId for response %d: %w", i, err)
		}

		instID, err := r.ReadNullableString()
		if err != nil {
			return nil, fmt.Errorf("failed to read GroupInstanceId for response %d: %w", i, err)
		}

		mErrCode, err := r.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed to read ErrorCode for response %d: %w", i, err)
		}

		memberResponses = append(memberResponses, LeaveMemberResponse{
			MemberId:        mid,
			GroupInstanceId: instID,
			ErrorCode:       mErrCode,
		})
	}

	resp := &LeaveGroupResponse{
		ThrottleTimeMs: throttleTimeMs,
		ErrorCode:      topErrCode,
		Members:        memberResponses,
	}

	if topErrCode != ErrNone {
		return resp, fmt.Errorf("LeaveGroup failed with error %d: %s", topErrCode, ErrorCodeText(topErrCode))
	}

	return resp, nil
}
