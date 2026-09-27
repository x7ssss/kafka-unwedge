package protocol

import (
	"fmt"
)

// API Keys for offset operations.
const (
	ApiKeyListOffsets int16 = 2
	ApiKeyOffsetFetch int16 = 9
)

// OffsetFetchPartitionResult represents committed offset for a partition.
type OffsetFetchPartitionResult struct {
	PartitionIndex  int32
	CommittedOffset int64
	Metadata        *string
	ErrorCode       int16
}

// OffsetFetchTopicResult contains partition committed offsets for a topic.
type OffsetFetchTopicResult struct {
	Topic      string
	Partitions []OffsetFetchPartitionResult
}

// ListOffsetsPartitionResult represents the Log End Offset (LEO) for a partition.
type ListOffsetsPartitionResult struct {
	PartitionIndex int32
	ErrorCode      int16
	Timestamp      int64
	Offset         int64
}

// ListOffsetsTopicResult contains partition LEOs for a topic.
type ListOffsetsTopicResult struct {
	Topic      string
	Partitions []ListOffsetsPartitionResult
}

// OffsetFetch executes OffsetFetch (API Key 9, Version 1) against the group coordinator.
func OffsetFetch(conn *Connection, groupID string, topicPartitions map[string][]int32) ([]OffsetFetchTopicResult, error) {
	w := NewWriter()
	w.WriteString(groupID)
	w.WriteArrayLen(len(topicPartitions))

	for topic, parts := range topicPartitions {
		w.WriteString(topic)
		w.WriteArrayLen(len(parts))
		for _, p := range parts {
			w.WriteInt32(p)
		}
	}

	respBytes, err := conn.SendRequest(ApiKeyOffsetFetch, 1, w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("OffsetFetch request failed: %w", err)
	}

	r := NewReader(respBytes)
	topicCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to read topic count from OffsetFetch response: %w", err)
	}

	results := make([]OffsetFetchTopicResult, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		tName, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read topic name: %w", err)
		}

		pCount, err := r.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("failed to read partition count for topic %s: %w", tName, err)
		}

		parts := make([]OffsetFetchPartitionResult, 0, pCount)
		for j := 0; j < int(pCount); j++ {
			pIdx, err := r.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition index: %w", err)
			}

			committedOff, err := r.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("failed to read committed offset: %w", err)
			}

			meta, err := r.ReadNullableString()
			if err != nil {
				return nil, fmt.Errorf("failed to read offset metadata: %w", err)
			}

			errCode, err := r.ReadInt16()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition error code: %w", err)
			}

			parts = append(parts, OffsetFetchPartitionResult{
				PartitionIndex:  pIdx,
				CommittedOffset: committedOff,
				Metadata:        meta,
				ErrorCode:       errCode,
			})
		}

		results = append(results, OffsetFetchTopicResult{
			Topic:      tName,
			Partitions: parts,
		})
	}

	return results, nil
}

// ListOffsets executes ListOffsets (API Key 2, Version 1) for a specific target broker.
// timestamp: -1 for Latest (LEO), -2 for Earliest.
func ListOffsets(conn *Connection, topicPartitions map[string][]int32, timestamp int64) ([]ListOffsetsTopicResult, error) {
	w := NewWriter()
	w.WriteInt32(-1) // ReplicaId (-1 for regular client)
	w.WriteArrayLen(len(topicPartitions))

	for topic, parts := range topicPartitions {
		w.WriteString(topic)
		w.WriteArrayLen(len(parts))
		for _, p := range parts {
			w.WriteInt32(p)
			w.WriteInt64(timestamp)
		}
	}

	respBytes, err := conn.SendRequest(ApiKeyListOffsets, 1, w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("ListOffsets request failed: %w", err)
	}

	r := NewReader(respBytes)
	topicCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to read topic count from ListOffsets response: %w", err)
	}

	results := make([]ListOffsetsTopicResult, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		tName, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read topic name: %w", err)
		}

		pCount, err := r.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("failed to read partition count for topic %s: %w", tName, err)
		}

		parts := make([]ListOffsetsPartitionResult, 0, pCount)
		for j := 0; j < int(pCount); j++ {
			pIdx, err := r.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition index: %w", err)
			}

			errCode, err := r.ReadInt16()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition error code: %w", err)
			}

			ts, err := r.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("failed to read timestamp: %w", err)
			}

			off, err := r.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("failed to read offset: %w", err)
			}

			parts = append(parts, ListOffsetsPartitionResult{
				PartitionIndex: pIdx,
				ErrorCode:      errCode,
				Timestamp:      ts,
				Offset:         off,
			})
		}

		results = append(results, ListOffsetsTopicResult{
			Topic:      tName,
			Partitions: parts,
		})
	}

	return results, nil
}

// FetchCommittedOffsets maps topic and partition to committed offset.
func FetchCommittedOffsets(conn *Connection, groupID string, topicPartitions map[string][]int32) (map[string]map[int32]int64, error) {
	res, err := OffsetFetch(conn, groupID, topicPartitions)
	if err != nil {
		return nil, err
	}

	out := make(map[string]map[int32]int64)
	for _, tr := range res {
		if _, ok := out[tr.Topic]; !ok {
			out[tr.Topic] = make(map[int32]int64)
		}
		for _, pr := range tr.Partitions {
			out[tr.Topic][pr.PartitionIndex] = pr.CommittedOffset
		}
	}
	return out, nil
}

// FetchLogEndOffsets queries cluster metadata and routes ListOffsets to partition leaders.
func FetchLogEndOffsets(conn *Connection, connCfg Config, topicPartitions map[string][]int32) (map[string]map[int32]int64, error) {
	topicNames := make([]string, 0, len(topicPartitions))
	for t := range topicPartitions {
		topicNames = append(topicNames, t)
	}

	out := make(map[string]map[int32]int64)
	for t := range topicPartitions {
		out[t] = make(map[int32]int64)
	}

	// Fetch cluster metadata to map partition -> leader broker.
	meta, err := FetchMetadata(conn, topicNames)
	if err != nil {
		// Fallback to querying current connection directly if metadata fails.
		rawList, rawErr := ListOffsets(conn, topicPartitions, -1)
		if rawErr != nil {
			return nil, fmt.Errorf("metadata lookup failed (%v) and fallback ListOffsets failed: %w", err, rawErr)
		}
		for _, tr := range rawList {
			for _, pr := range tr.Partitions {
				out[tr.Topic][pr.PartitionIndex] = pr.Offset
			}
		}
		return out, nil
	}

	// Map broker ID to address.
	brokerMap := make(map[int32]string)
	for _, b := range meta.Brokers {
		brokerMap[b.NodeID] = b.Addr()
	}

	// Group partitions by leader broker ID.
	type partKey struct {
		topic     string
		partition int32
	}
	brokerPartitions := make(map[int32]map[string][]int32)

	for _, tm := range meta.Topics {
		requestedParts, topicRequested := topicPartitions[tm.Name]
		if !topicRequested {
			continue
		}

		reqSet := make(map[int32]bool)
		for _, p := range requestedParts {
			reqSet[p] = true
		}

		for _, pm := range tm.Partitions {
			if len(requestedParts) > 0 && !reqSet[pm.PartitionID] {
				continue
			}

			leader := pm.LeaderID
			if _, ok := brokerPartitions[leader]; !ok {
				brokerPartitions[leader] = make(map[string][]int32)
			}
			brokerPartitions[leader][tm.Name] = append(brokerPartitions[leader][tm.Name], pm.PartitionID)
		}
	}

	// Query each leader broker.
	for leaderID, tpMap := range brokerPartitions {
		targetAddr, hasAddr := brokerMap[leaderID]
		var targetConn *Connection
		var shouldClose bool

		if hasAddr && targetAddr != "" {
			leaderCfg := connCfg
			leaderCfg.Addr = targetAddr
			dialedConn, dialErr := Dial(leaderCfg)
			if dialErr == nil {
				targetConn = dialedConn
				shouldClose = true
			}
		}

		if targetConn == nil {
			// Fallback to the primary connection.
			targetConn = conn
			shouldClose = false
		}

		loResp, loErr := ListOffsets(targetConn, tpMap, -1)
		if shouldClose {
			_ = targetConn.Close()
		}

		if loErr != nil {
			return nil, fmt.Errorf("failed to fetch LEO from leader broker %d: %w", leaderID, loErr)
		}

		for _, tr := range loResp {
			for _, pr := range tr.Partitions {
				out[tr.Topic][pr.PartitionIndex] = pr.Offset
			}
		}
	}

	return out, nil
}
