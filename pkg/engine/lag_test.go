package engine

import (
	"testing"
	"time"
)

func TestComputeLag(t *testing.T) {
	cases := []struct {
		name      string
		leo       int64
		committed int64
		expected  int64
	}{
		{"Normal lag", 1500, 1200, 300},
		{"Zero lag fully caught up", 2000, 2000, 0},
		{"Uncommitted partition with data", 500, -1, 500},
		{"Uncommitted empty partition", -1, -1, 0},
		{"LEO behind committed (compaction/edge)", 100, 150, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := ComputeLag(c.leo, c.committed)
			if actual != c.expected {
				t.Fatalf("ComputeLag(%d, %d) = %d, expected %d", c.leo, c.committed, actual, c.expected)
			}
		})
	}
}

func TestComputeDifferential(t *testing.T) {
	window := 5 * time.Second

	// Case 1: STALLED (Lag > 0, DeltaCommit == 0)
	s1Stalled := PartitionSnapshot{
		Topic:           "orders",
		Partition:       0,
		CommittedOffset: 1000,
		LogEndOffset:    1500,
		Lag:             500,
	}
	s2Stalled := PartitionSnapshot{
		Topic:           "orders",
		Partition:       0,
		CommittedOffset: 1000, // No progress made
		LogEndOffset:    1600, // LEO increased
		Lag:             600,
	}
	diffStalled := ComputeDifferential(s1Stalled, s2Stalled, window)
	if diffStalled.Status != StatusStalled {
		t.Errorf("Expected STALLED status, got %s", diffStalled.Status)
	}
	if diffStalled.CommitVelocity != 0.0 {
		t.Errorf("Expected 0.0 commit velocity, got %f", diffStalled.CommitVelocity)
	}
	if diffStalled.LagVelocity != 20.0 { // (600 - 500) / 5 = 20.0
		t.Errorf("Expected 20.0 lag velocity, got %f", diffStalled.LagVelocity)
	}

	// Case 2: FALLING_BEHIND (DeltaLag > 0, commits occur but slower than ingestion)
	s1Falling := PartitionSnapshot{
		Topic:           "orders",
		Partition:       1,
		CommittedOffset: 1000,
		LogEndOffset:    1200,
		Lag:             200,
	}
	s2Falling := PartitionSnapshot{
		Topic:           "orders",
		Partition:       1,
		CommittedOffset: 1050, // +50 commits
		LogEndOffset:    1400, // +200 LEO
		Lag:             350,  // +150 lag
	}
	diffFalling := ComputeDifferential(s1Falling, s2Falling, window)
	if diffFalling.Status != StatusFallingBehind {
		t.Errorf("Expected FALLING_BEHIND status, got %s", diffFalling.Status)
	}
	if diffFalling.CommitVelocity != 10.0 { // 50 / 5 = 10.0
		t.Errorf("Expected 10.0 commit velocity, got %f", diffFalling.CommitVelocity)
	}
	if diffFalling.LagVelocity != 30.0 { // 150 / 5 = 30.0
		t.Errorf("Expected 30.0 lag velocity, got %f", diffFalling.LagVelocity)
	}

	// Case 3: HEALTHY Draining (Lag > 0, but DeltaLag < 0 and DeltaCommit > 0)
	s1Draining := PartitionSnapshot{
		Topic:           "orders",
		Partition:       2,
		CommittedOffset: 1000,
		LogEndOffset:    1500,
		Lag:             500,
	}
	s2Draining := PartitionSnapshot{
		Topic:           "orders",
		Partition:       2,
		CommittedOffset: 1300, // +300 commits
		LogEndOffset:    1600, // +100 LEO
		Lag:             300,  // -200 lag
	}
	diffDraining := ComputeDifferential(s1Draining, s2Draining, window)
	if diffDraining.Status != StatusHealthy {
		t.Errorf("Expected HEALTHY status for draining partition, got %s", diffDraining.Status)
	}
	if diffDraining.CommitVelocity != 60.0 {
		t.Errorf("Expected 60.0 commit velocity, got %f", diffDraining.CommitVelocity)
	}
	if diffDraining.LagVelocity != -40.0 {
		t.Errorf("Expected -40.0 lag velocity, got %f", diffDraining.LagVelocity)
	}

	// Case 4: HEALTHY Zero Lag
	s1CaughtUp := PartitionSnapshot{
		Topic:           "orders",
		Partition:       3,
		CommittedOffset: 5000,
		LogEndOffset:    5000,
		Lag:             0,
	}
	s2CaughtUp := PartitionSnapshot{
		Topic:           "orders",
		Partition:       3,
		CommittedOffset: 5050,
		LogEndOffset:    5050,
		Lag:             0,
	}
	diffCaughtUp := ComputeDifferential(s1CaughtUp, s2CaughtUp, window)
	if diffCaughtUp.Status != StatusHealthy {
		t.Errorf("Expected HEALTHY status for zero lag, got %s", diffCaughtUp.Status)
	}
}

func TestAnalyzeSnapshots(t *testing.T) {
	window := 2 * time.Second

	snap1 := map[string]map[int32]PartitionSnapshot{
		"events": {
			0: {Topic: "events", Partition: 0, CommittedOffset: 100, LogEndOffset: 200, Lag: 100},
			1: {Topic: "events", Partition: 1, CommittedOffset: 100, LogEndOffset: 100, Lag: 0},
			2: {Topic: "events", Partition: 2, CommittedOffset: 100, LogEndOffset: 200, Lag: 100},
		},
	}

	snap2 := map[string]map[int32]PartitionSnapshot{
		"events": {
			// Partition 0 is stalled (no commit change, positive lag)
			0: {Topic: "events", Partition: 0, CommittedOffset: 100, LogEndOffset: 250, Lag: 150},
			// Partition 1 is healthy (zero lag)
			1: {Topic: "events", Partition: 1, CommittedOffset: 120, LogEndOffset: 120, Lag: 0},
			// Partition 2 is falling behind (lag increased from 100 to 180)
			2: {Topic: "events", Partition: 2, CommittedOffset: 110, LogEndOffset: 290, Lag: 180},
		},
	}

	summary := AnalyzeSnapshots(snap1, snap2, window)

	if summary.TotalPartitions != 3 {
		t.Fatalf("Expected 3 partitions, got %d", summary.TotalPartitions)
	}
	if summary.StalledPartitions != 1 {
		t.Errorf("Expected 1 stalled partition, got %d", summary.StalledPartitions)
	}
	if summary.FallingPartitions != 1 {
		t.Errorf("Expected 1 falling partition, got %d", summary.FallingPartitions)
	}
	if summary.HealthyPartitions != 1 {
		t.Errorf("Expected 1 healthy partition, got %d", summary.HealthyPartitions)
	}
	if summary.OverallStatus != StatusStalled {
		t.Errorf("Expected overall status STALLED when stalled partitions exist, got %s", summary.OverallStatus)
	}
	if summary.TotalLag1 != 200 {
		t.Errorf("Expected TotalLag1 = 200, got %d", summary.TotalLag1)
	}
	if summary.TotalLag2 != 330 {
		t.Errorf("Expected TotalLag2 = 330, got %d", summary.TotalLag2)
	}
	if summary.DeltaLag != 130 {
		t.Errorf("Expected DeltaLag = 130, got %d", summary.DeltaLag)
	}

	// Verify sorting order: partition 0, then 1, then 2
	for i := 0; i < 3; i++ {
		if summary.Partitions[i].Partition != int32(i) {
			t.Errorf("Expected partition index %d at position %d, got %d", i, i, summary.Partitions[i].Partition)
		}
	}
}
