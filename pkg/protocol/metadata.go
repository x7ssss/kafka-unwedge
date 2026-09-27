package protocol

import (
	"fmt"
	"net"
)

// API Key for Metadata.
const ApiKeyMetadata int16 = 3

// BrokerMetadata represents a broker in the Kafka cluster.
type BrokerMetadata struct {
	NodeID int32
	Host   string
	Port   int32
}

// Addr returns host:port formatted string.
func (b *BrokerMetadata) Addr() string {
	return net.JoinHostPort(b.Host, fmt.Sprintf("%d", b.Port))
}

// PartitionMetadata represents a partition's state and leadership.
type PartitionMetadata struct {
	ErrorCode    int16
	PartitionID  int32
	LeaderID     int32
	ReplicaNodes []int32
	IsrNodes     []int32
}

// TopicMetadata represents a topic and its partitions.
type TopicMetadata struct {
	ErrorCode  int16
	Name       string
	Partitions []PartitionMetadata
}

// ClusterMetadata contains brokers and topic partition leadership info.
type ClusterMetadata struct {
	Brokers []BrokerMetadata
	Topics  []TopicMetadata
}

// FetchMetadata executes MetadataRequest (API Key 3, Version 0).
func FetchMetadata(conn *Connection, topics []string) (*ClusterMetadata, error) {
	w := NewWriter()
	w.WriteArrayLen(len(topics))
	for _, t := range topics {
		w.WriteString(t)
	}

	respBytes, err := conn.SendRequest(ApiKeyMetadata, 0, w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Metadata request failed: %w", err)
	}

	r := NewReader(respBytes)

	brokerCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to read broker count: %w", err)
	}

	brokers := make([]BrokerMetadata, 0, brokerCount)
	for i := 0; i < int(brokerCount); i++ {
		nodeID, err := r.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed to read broker NodeId: %w", err)
		}

		host, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read broker Host: %w", err)
		}

		port, err := r.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed to read broker Port: %w", err)
		}

		brokers = append(brokers, BrokerMetadata{
			NodeID: nodeID,
			Host:   host,
			Port:   port,
		})
	}

	topicCount, err := r.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to read topic count: %w", err)
	}

	topicMetas := make([]TopicMetadata, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		errCode, err := r.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed to read topic ErrorCode: %w", err)
		}

		topicName, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read topic Name: %w", err)
		}

		partCount, err := r.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("failed to read partition count for topic %s: %w", topicName, err)
		}

		partitions := make([]PartitionMetadata, 0, partCount)
		for j := 0; j < int(partCount); j++ {
			pErrCode, err := r.ReadInt16()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition ErrorCode: %w", err)
			}

			pID, err := r.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition ID: %w", err)
			}

			leaderID, err := r.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed to read partition Leader: %w", err)
			}

			repCount, err := r.ReadArrayLen()
			if err != nil {
				return nil, fmt.Errorf("failed to read replica count: %w", err)
			}

			replicas := make([]int32, 0, repCount)
			for k := 0; k < int(repCount); k++ {
				rep, err := r.ReadInt32()
				if err != nil {
					return nil, fmt.Errorf("failed to read replica: %w", err)
				}
				replicas = append(replicas, rep)
			}

			isrCount, err := r.ReadArrayLen()
			if err != nil {
				return nil, fmt.Errorf("failed to read isr count: %w", err)
			}

			isrs := make([]int32, 0, isrCount)
			for k := 0; k < int(isrCount); k++ {
				isr, err := r.ReadInt32()
				if err != nil {
					return nil, fmt.Errorf("failed to read isr: %w", err)
				}
				isrs = append(isrs, isr)
			}

			partitions = append(partitions, PartitionMetadata{
				ErrorCode:    pErrCode,
				PartitionID:  pID,
				LeaderID:     leaderID,
				ReplicaNodes: replicas,
				IsrNodes:     isrs,
			})
		}

		topicMetas = append(topicMetas, TopicMetadata{
			ErrorCode:  errCode,
			Name:       topicName,
			Partitions: partitions,
		})
	}

	return &ClusterMetadata{
		Brokers: brokers,
		Topics:  topicMetas,
	}, nil
}
