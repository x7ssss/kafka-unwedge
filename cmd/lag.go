package cmd

import (
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/x7ssss/kafka-unwedge/pkg/engine"
	"github.com/x7ssss/kafka-unwedge/pkg/protocol"
	"github.com/x7ssss/kafka-unwedge/pkg/ui"
)

var (
	lagBroker string
	lagGroup  string
	lagWindow time.Duration
	lagTLS    bool
)

var lagCmd = &cobra.Command{
	Use:   "lag",
	Short: "Differential lag velocity analyzer measuring dLag/dt over temporal windows",
	Long: `Samples consumer group committed offsets and Log End Offsets (LEO) across two temporal
points separated by an observation window, computing commit velocity and lag acceleration
to classify partitions as HEALTHY, FALLING_BEHIND, or STALLED.`,
	RunE: runLag,
}

func init() {
	lagCmd.Flags().StringVar(&lagBroker, "broker", "", "Kafka bootstrap broker address (host:port)")
	lagCmd.Flags().StringVar(&lagGroup, "group", "", "Kafka consumer group ID to analyze")
	lagCmd.Flags().DurationVar(&lagWindow, "window", 5*time.Second, "Temporal sampling window (e.g. 5s)")
	lagCmd.Flags().BoolVar(&lagTLS, "tls", false, "Enable TLS connection to Kafka broker")

	_ = lagCmd.MarkFlagRequired("broker")
	_ = lagCmd.MarkFlagRequired("group")

	RootCmd.AddCommand(lagCmd)
}

func runLag(cmd *cobra.Command, args []string) error {
	tlsStr := "DISABLED"
	if lagTLS {
		tlsStr = "ENABLED (TLS 1.2+)"
	}

	bannerMeta := [][2]string{
		{"OPERATION", "DIFFERENTIAL LAG ANALYZER (dLag/dt)"},
		{"BOOTSTRAP BROKER", lagBroker},
		{"TARGET GROUP", lagGroup},
		{"OBSERVATION WINDOW", lagWindow.String()},
		{"SECURITY", tlsStr},
	}
	fmt.Print(ui.Banner("KAFKA-UNWEDGE :: DIFFERENTIAL LAG VELOCITY", bannerMeta))

	cfg := createConnectionConfig(lagBroker, lagTLS, 10*time.Second)

	// Step 1: Discover Group Coordinator.
	fmt.Printf("[PHASE 1] Resolving group coordinator for group '%s'...\n", lagGroup)
	bootstrapConn, err := protocol.Dial(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to bootstrap broker: %w", err)
	}
	defer bootstrapConn.Close()

	coord, err := protocol.FindCoordinator(bootstrapConn, lagGroup)
	if err != nil {
		return fmt.Errorf("FindCoordinator failed: %w", err)
	}

	coordAddr := coord.Addr()
	fmt.Printf("[COORDINATOR] Node ID %d at %s\n\n", coord.NodeID, coordAddr)

	// Step 2: Connect to coordinator and discover group topology.
	coordCfg := cfg
	coordCfg.Addr = coordAddr
	coordConn, err := protocol.Dial(coordCfg)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator %s: %w", coordAddr, err)
	}
	defer coordConn.Close()

	group, err := protocol.DescribeGroup(coordConn, lagGroup)
	if err != nil {
		return fmt.Errorf("DescribeGroup failed: %w", err)
	}

	topicPartMap := make(map[string]map[int32]bool)
	topicSet := make(map[string]bool)

	for _, m := range group.Members {
		for _, a := range m.Assignments {
			topicSet[a.Topic] = true
			if _, ok := topicPartMap[a.Topic]; !ok {
				topicPartMap[a.Topic] = make(map[int32]bool)
			}
			for _, p := range a.Partitions {
				topicPartMap[a.Topic][p] = true
			}
		}
		for _, t := range m.SubscribedTopics {
			topicSet[t] = true
		}
	}

	// Query cluster metadata to ensure all partitions of subscribed topics are discovered.
	var topicNames []string
	for t := range topicSet {
		topicNames = append(topicNames, t)
	}

	if len(topicNames) > 0 {
		meta, metaErr := protocol.FetchMetadata(bootstrapConn, topicNames)
		if metaErr == nil {
			for _, tm := range meta.Topics {
				if _, ok := topicPartMap[tm.Name]; !ok {
					topicPartMap[tm.Name] = make(map[int32]bool)
				}
				for _, pm := range tm.Partitions {
					topicPartMap[tm.Name][pm.PartitionID] = true
				}
			}
		}
	}

	if len(topicPartMap) == 0 {
		fmt.Println("[WARNING] No topic partitions discovered for this consumer group.")
		fmt.Println("The group may be Empty, inactive, or partitions have not been assigned yet.")
		return nil
	}

	topicPartitions := make(map[string][]int32)
	for t, pMap := range topicPartMap {
		var parts []int32
		for p := range pMap {
			parts = append(parts, p)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		topicPartitions[t] = parts
	}

	// Step 3: Take Sample 1 (t0).
	fmt.Printf("[SAMPLING] Collecting Sample 1 at t0 across %d topics...\n", len(topicPartitions))
	t0 := time.Now()

	committed1, err := protocol.FetchCommittedOffsets(coordConn, lagGroup, topicPartitions)
	if err != nil {
		return fmt.Errorf("failed to fetch committed offsets for sample 1: %w", err)
	}

	leo1, err := protocol.FetchLogEndOffsets(bootstrapConn, cfg, topicPartitions)
	if err != nil {
		return fmt.Errorf("failed to fetch LEOs for sample 1: %w", err)
	}

	snap1 := make(map[string]map[int32]engine.PartitionSnapshot)
	for t, parts := range topicPartitions {
		snap1[t] = make(map[int32]engine.PartitionSnapshot)
		for _, p := range parts {
			cOff := int64(-1)
			if m, ok := committed1[t]; ok {
				if val, ok2 := m[p]; ok2 {
					cOff = val
				}
			}
			lOff := int64(0)
			if m, ok := leo1[t]; ok {
				if val, ok2 := m[p]; ok2 {
					lOff = val
				}
			}
			lag := engine.ComputeLag(lOff, cOff)
			snap1[t][p] = engine.PartitionSnapshot{
				Topic:           t,
				Partition:       p,
				CommittedOffset: cOff,
				LogEndOffset:    lOff,
				Lag:             lag,
				Timestamp:       t0,
			}
		}
	}

	// Step 4: Temporal Window Wait.
	fmt.Printf("[SAMPLING] Waiting temporal observation window (%v)...\n", lagWindow)
	time.Sleep(lagWindow)

	// Step 5: Take Sample 2 (t1).
	t1 := time.Now()
	actualElapsed := t1.Sub(t0)
	fmt.Printf("[SAMPLING] Collecting Sample 2 at t1 (elapsed %v)...\n\n", actualElapsed.Round(time.Millisecond))

	committed2, err := protocol.FetchCommittedOffsets(coordConn, lagGroup, topicPartitions)
	if err != nil {
		return fmt.Errorf("failed to fetch committed offsets for sample 2: %w", err)
	}

	leo2, err := protocol.FetchLogEndOffsets(bootstrapConn, cfg, topicPartitions)
	if err != nil {
		return fmt.Errorf("failed to fetch LEOs for sample 2: %w", err)
	}

	snap2 := make(map[string]map[int32]engine.PartitionSnapshot)
	for t, parts := range topicPartitions {
		snap2[t] = make(map[int32]engine.PartitionSnapshot)
		for _, p := range parts {
			cOff := int64(-1)
			if m, ok := committed2[t]; ok {
				if val, ok2 := m[p]; ok2 {
					cOff = val
				}
			}
			lOff := int64(0)
			if m, ok := leo2[t]; ok {
				if val, ok2 := m[p]; ok2 {
					lOff = val
				}
			}
			lag := engine.ComputeLag(lOff, cOff)
			snap2[t][p] = engine.PartitionSnapshot{
				Topic:           t,
				Partition:       p,
				CommittedOffset: cOff,
				LogEndOffset:    lOff,
				Lag:             lag,
				Timestamp:       t1,
			}
		}
	}

	// Step 6: Compute Differential and Classify.
	summary := engine.AnalyzeSnapshots(snap1, snap2, actualElapsed)

	// Step 7: Monospace Brutalist Output Table.
	table := ui.NewTable(
		"TOPIC",
		"PART",
		"COMMITTED",
		"LEO",
		"LAG",
		"dCOMMIT/dt",
		"dLAG/dt",
		"STATUS",
	)
	table.SetAlignment(
		ui.AlignLeft,
		ui.AlignRight,
		ui.AlignRight,
		ui.AlignRight,
		ui.AlignRight,
		ui.AlignRight,
		ui.AlignRight,
		ui.AlignCenter,
	)

	for _, d := range summary.Partitions {
		cStr := fmt.Sprintf("%d", d.Committed2)
		if d.Committed2 < 0 {
			cStr = "(none)"
		}

		table.AddRow(
			d.Topic,
			fmt.Sprintf("%d", d.Partition),
			cStr,
			fmt.Sprintf("%d", d.LEO2),
			fmt.Sprintf("%d", d.Lag2),
			fmt.Sprintf("%+.1f msg/s", d.CommitVelocity),
			fmt.Sprintf("%+.1f msg/s", d.LagVelocity),
			ui.StatusBadge(d.Status),
		)
	}

	fmt.Print(table.Render())
	fmt.Println()

	// Step 8: Group Summary Card.
	fmt.Println("+============================================================================+")
	fmt.Println("| DIFFERENTIAL LAG SUMMARY & VELOCITY ASSESSMENT                             |")
	fmt.Println("+----------------------------------------------------------------------------+")
	fmt.Printf("| Total Partitions Evaluated : %-45d |\n", summary.TotalPartitions)
	fmt.Printf("| Healthy Partitions         : %-45d |\n", summary.HealthyPartitions)
	fmt.Printf("| Falling Behind Partitions  : %-45d |\n", summary.FallingPartitions)
	fmt.Printf("| Stalled / Wedged Partitions: %-45d |\n", summary.StalledPartitions)
	fmt.Printf("| Initial Total Lag (t0)     : %-45d |\n", summary.TotalLag1)
	fmt.Printf("| Current Total Lag (t1)     : %-45d |\n", summary.TotalLag2)
	fmt.Printf("| Net Lag Change (Delta Lag) : %-+45d |\n", summary.DeltaLag)
	fmt.Printf("| Overall Group Health Status: %-45s |\n", ui.StatusBadge(summary.OverallStatus))
	fmt.Println("+----------------------------------------------------------------------------+")

	if summary.StalledPartitions > 0 {
		fmt.Println("| CRITICAL WARNING: ZOMBIE OR WEDGED CONSUMERS DETECTED                      |")
		fmt.Println("| Partitions have non-zero lag but zero commit progress over the window.     |")
		fmt.Println("| This indicates stuck worker goroutines, deadlocked locks, or silent faults.|")
		fmt.Println("| ACTION: Inspect member IDs and execute 'kafka-unwedge fence' to evict.     |")
	} else if summary.FallingPartitions > 0 {
		fmt.Println("| WARNING: CONSUMERS FALLING BEHIND INCOMING INGESTION RATE                  |")
		fmt.Println("| Log End Offsets are growing faster than consumer commits (dLag/dt > 0).    |")
		fmt.Println("| ACTION: Scale out consumer instances or optimize consumer processing logic.|")
	} else {
		fmt.Println("| HEALTHY: All partitions are either fully caught up or actively draining.   |")
	}
	fmt.Println("+============================================================================+")

	return nil
}
