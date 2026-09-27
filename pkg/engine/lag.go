package engine

import (
	"sort"
	"time"
)

// Partition status classifications.
const (
	StatusHealthy       = "HEALTHY"
	StatusFallingBehind = "FALLING_BEHIND"
	StatusStalled       = "STALLED"
)

// PartitionSnapshot captures offset state at a point in time.
type PartitionSnapshot struct {
	Topic           string
	Partition       int32
	CommittedOffset int64
	LogEndOffset    int64
	Lag             int64
	Timestamp       time.Time
}

// ComputeLag calculates lag given LEO and committed offset.
func ComputeLag(leo, committed int64) int64 {
	if committed < 0 {
		if leo < 0 {
			return 0
		}
		return leo
	}
	if leo < committed {
		return 0
	}
	return leo - committed
}

// PartitionDifferential represents temporal rate-of-change metrics for a single partition.
type PartitionDifferential struct {
	Topic          string
	Partition      int32
	Committed1     int64
	Committed2     int64
	DeltaCommit    int64
	LEO1           int64
	LEO2           int64
	DeltaLEO       int64
	Lag1           int64
	Lag2           int64
	DeltaLag       int64
	Window         time.Duration
	CommitVelocity float64 // dCommit/dt (msgs/sec)
	LagVelocity    float64 // dLag/dt (msgs/sec)
	Status         string  // HEALTHY, FALLING_BEHIND, STALLED
}

// ComputeDifferential compares two snapshots taken over a time window.
func ComputeDifferential(s1, s2 PartitionSnapshot, window time.Duration) PartitionDifferential {
	dtSec := window.Seconds()
	if dtSec <= 0 {
		dtSec = 1.0
	}

	deltaCommit := s2.CommittedOffset - s1.CommittedOffset
	if s1.CommittedOffset < 0 && s2.CommittedOffset >= 0 {
		deltaCommit = s2.CommittedOffset
	}

	deltaLEO := s2.LogEndOffset - s1.LogEndOffset
	deltaLag := s2.Lag - s1.Lag

	commitVelocity := float64(deltaCommit) / dtSec
	lagVelocity := float64(deltaLag) / dtSec

	var status string
	if s2.Lag <= 0 {
		status = StatusHealthy
	} else if deltaCommit == 0 && s2.Lag > 0 {
		status = StatusStalled
	} else if deltaLag > 0 {
		status = StatusFallingBehind
	} else {
		status = StatusHealthy
	}

	return PartitionDifferential{
		Topic:          s2.Topic,
		Partition:      s2.Partition,
		Committed1:     s1.CommittedOffset,
		Committed2:     s2.CommittedOffset,
		DeltaCommit:    deltaCommit,
		LEO1:           s1.LogEndOffset,
		LEO2:           s2.LogEndOffset,
		DeltaLEO:       deltaLEO,
		Lag1:           s1.Lag,
		Lag2:           s2.Lag,
		DeltaLag:       deltaLag,
		Window:         window,
		CommitVelocity: commitVelocity,
		LagVelocity:    lagVelocity,
		Status:         status,
	}
}

// GroupSummary aggregates differential metrics across all evaluated partitions.
type GroupSummary struct {
	TotalPartitions   int
	HealthyPartitions int
	FallingPartitions int
	StalledPartitions int
	TotalLag1         int64
	TotalLag2         int64
	DeltaLag          int64
	OverallStatus     string
	Partitions        []PartitionDifferential
}

// AnalyzeSnapshots processes two complete snapshot sets and produces partition differentials.
func AnalyzeSnapshots(
	s1 map[string]map[int32]PartitionSnapshot,
	s2 map[string]map[int32]PartitionSnapshot,
	window time.Duration,
) *GroupSummary {
	var diffs []PartitionDifferential

	for topic, parts := range s2 {
		for pIdx, snap2 := range parts {
			snap1, exists := s1[topic][pIdx]
			if !exists {
				// If partition wasn't in sample 1, treat sample 1 as equivalent baseline.
				snap1 = snap2
			}
			diff := ComputeDifferential(snap1, snap2, window)
			diffs = append(diffs, diff)
		}
	}

	// Stable sort by Topic, then Partition.
	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].Topic == diffs[j].Topic {
			return diffs[i].Partition < diffs[j].Partition
		}
		return diffs[i].Topic < diffs[j].Topic
	})

	summary := &GroupSummary{
		TotalPartitions: len(diffs),
		Partitions:      diffs,
		OverallStatus:   StatusHealthy,
	}

	for _, d := range diffs {
		summary.TotalLag1 += d.Lag1
		summary.TotalLag2 += d.Lag2
		switch d.Status {
		case StatusStalled:
			summary.StalledPartitions++
		case StatusFallingBehind:
			summary.FallingPartitions++
		case StatusHealthy:
			summary.HealthyPartitions++
		}
	}
	summary.DeltaLag = summary.TotalLag2 - summary.TotalLag1

	if summary.StalledPartitions > 0 {
		summary.OverallStatus = StatusStalled
	} else if summary.FallingPartitions > 0 {
		summary.OverallStatus = StatusFallingBehind
	} else {
		summary.OverallStatus = StatusHealthy
	}

	return summary
}
