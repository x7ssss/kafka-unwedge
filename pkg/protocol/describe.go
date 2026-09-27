package protocol

import (
	"fmt"
)

// API Key for DescribeGroups.
const ApiKeyDescribeGroups int16 = 15

// DescribedMember contains details of a group member.
type DescribedMember struct {
	MemberId         string
	ClientId         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte

	// Parsed consumer protocol fields (populated when applicable).
	SubscribedTopics []string
	Assignments      []TopicAssignment
}

// TopicAssignment maps a topic to assigned partition indexes.
type TopicAssignment struct {
	Topic      string
	Partitions []int32
}

// DescribedGroup contains coordinator state and member list for a consumer group.
type DescribedGroup struct {
	ErrorCode    int16
	GroupId      string
	GroupState   string
	ProtocolType string
	ProtocolData string
	Members      []DescribedMember
}

// DescribeGroups executes DescribeGroups (API Key 15, Version 0).
func DescribeGroups(conn *Connection, groupIDs []string) ([]DescribedGroup, error) {
	w := NewWriter()
	w.WriteArrayLen(len(groupIDs))
	for _, gid := range groupIDs {
		w.WriteString(gid)
	}

	respBytes, err := conn.SendRequest(ApiKeyDescribeGroups, 0, w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("DescribeGroups request failed: %w", err)
	}

	r := NewReader(respBytes)
	groupCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to read group count: %w", err)
	}

	groups := make([]DescribedGroup, 0, groupCount)
	for i := 0; i < int(groupCount); i++ {
		errCode, err := r.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed to read ErrorCode for group %d: %w", i, err)
		}

		groupID, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read GroupId for group %d: %w", i, err)
		}

		state, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read GroupState for group %d: %w", i, err)
		}

		protocolType, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read ProtocolType for group %d: %w", i, err)
		}

		protocolData, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read ProtocolData for group %d: %w", i, err)
		}

		memberCount, err := r.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("failed to read member count for group %s: %w", groupID, err)
		}

		members := make([]DescribedMember, 0, memberCount)
		for j := 0; j < int(memberCount); j++ {
			memberID, err := r.ReadString()
			if err != nil {
				return nil, fmt.Errorf("failed to read MemberId for member %d: %w", j, err)
			}

			clientID, err := r.ReadString()
			if err != nil {
				return nil, fmt.Errorf("failed to read ClientId for member %d: %w", j, err)
			}

			clientHost, err := r.ReadString()
			if err != nil {
				return nil, fmt.Errorf("failed to read ClientHost for member %d: %w", j, err)
			}

			memberMetadata, err := r.ReadBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to read MemberMetadata for member %d: %w", j, err)
			}

			memberAssignment, err := r.ReadBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to read MemberAssignment for member %d: %w", j, err)
			}

			member := DescribedMember{
				MemberId:         memberID,
				ClientId:         clientID,
				ClientHost:       clientHost,
				MemberMetadata:   memberMetadata,
				MemberAssignment: memberAssignment,
			}

			// Attempt parsing consumer metadata (subscription) and assignment.
			if len(memberMetadata) > 0 {
				if topics, parseErr := parseConsumerProtocolSubscription(memberMetadata); parseErr == nil {
					member.SubscribedTopics = topics
				}
			}

			if len(memberAssignment) > 0 {
				if assignments, parseErr := parseConsumerProtocolAssignment(memberAssignment); parseErr == nil {
					member.Assignments = assignments
				}
			}

			members = append(members, member)
		}

		groups = append(groups, DescribedGroup{
			ErrorCode:    errCode,
			GroupId:      groupID,
			GroupState:   state,
			ProtocolType: protocolType,
			ProtocolData: protocolData,
			Members:      members,
		})
	}

	return groups, nil
}

// DescribeGroup is a convenience helper for querying a single consumer group.
func DescribeGroup(conn *Connection, groupID string) (*DescribedGroup, error) {
	groups, err := DescribeGroups(conn, []string{groupID})
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("no group data returned for group: %s", groupID)
	}

	g := groups[0]
	if g.ErrorCode != ErrNone {
		return &g, fmt.Errorf("DescribeGroups error for %s (code %d): %s", groupID, g.ErrorCode, ErrorCodeText(g.ErrorCode))
	}
	return &g, nil
}

// parseConsumerProtocolSubscription decodes standard Kafka consumer subscription metadata:
// version: int16
// topics: []string
// userData: bytes
func parseConsumerProtocolSubscription(data []byte) ([]string, error) {
	r := NewReader(data)
	_, err := r.ReadInt16() // version
	if err != nil {
		return nil, err
	}

	topicCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	topics := make([]string, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		t, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		topics = append(topics, t)
	}
	return topics, nil
}

// parseConsumerProtocolAssignment decodes standard Kafka consumer partition assignment:
// version: int16
// topicPartitions: []struct{ Topic string, Partitions []int32 }
// userData: bytes
func parseConsumerProtocolAssignment(data []byte) ([]TopicAssignment, error) {
	r := NewReader(data)
	_, err := r.ReadInt16() // version
	if err != nil {
		return nil, err
	}

	topicCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	assignments := make([]TopicAssignment, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		topic, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		partCount, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}

		partitions := make([]int32, 0, partCount)
		for j := 0; j < int(partCount); j++ {
			p, err := r.ReadInt32()
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, p)
		}

		assignments = append(assignments, TopicAssignment{
			Topic:      topic,
			Partitions: partitions,
		})
	}

	return assignments, nil
}
