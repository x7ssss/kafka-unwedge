package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/x7ssss/kafka-unwedge/pkg/protocol"
	"github.com/x7ssss/kafka-unwedge/pkg/ui"
)

var (
	auditBroker   string
	auditGroup    string
	auditSamples  int
	auditInterval time.Duration
	auditTLS      bool
)

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Audit consumer group coordinator state machine and detect rebalance storms",
	Long: `Polls the consumer group coordinator state machine in real time across temporal samples,
detecting rebalance flapping, eager partition revocations, and group instability.`,
	RunE: runAudit,
}

func init() {
	auditCmd.Flags().StringVar(&auditBroker, "broker", "", "Kafka bootstrap broker address (host:port)")
	auditCmd.Flags().StringVar(&auditGroup, "group", "", "Kafka consumer group ID to audit")
	auditCmd.Flags().IntVar(&auditSamples, "samples", 10, "Number of state samples to record")
	auditCmd.Flags().DurationVar(&auditInterval, "interval", 1*time.Second, "Polling interval between samples")
	auditCmd.Flags().BoolVar(&auditTLS, "tls", false, "Enable TLS connection to Kafka broker")

	_ = auditCmd.MarkFlagRequired("broker")
	_ = auditCmd.MarkFlagRequired("group")

	RootCmd.AddCommand(auditCmd)
}

func runAudit(cmd *cobra.Command, args []string) error {
	tlsStr := "DISABLED"
	if auditTLS {
		tlsStr = "ENABLED (TLS 1.2+)"
	}

	bannerMeta := [][2]string{
		{"OPERATION", "AUDIT GROUP COORDINATOR"},
		{"BOOTSTRAP BROKER", auditBroker},
		{"TARGET GROUP", auditGroup},
		{"SAMPLE COUNT", fmt.Sprintf("%d", auditSamples)},
		{"POLL INTERVAL", auditInterval.String()},
		{"SECURITY", tlsStr},
	}
	fmt.Print(ui.Banner("KAFKA-UNWEDGE :: REBALANCE STORM DETECTOR", bannerMeta))

	cfg := createConnectionConfig(auditBroker, auditTLS, 10*time.Second)

	// Step 1: Discover Group Coordinator.
	fmt.Printf("[PHASE 1] Resolving group coordinator for group '%s'...\n", auditGroup)
	bootstrapConn, err := protocol.Dial(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to bootstrap broker: %w", err)
	}
	defer bootstrapConn.Close()

	coord, err := protocol.FindCoordinator(bootstrapConn, auditGroup)
	if err != nil {
		return fmt.Errorf("FindCoordinator failed: %w", err)
	}

	coordAddr := coord.Addr()
	fmt.Printf("[COORDINATOR] Node ID %d at %s\n\n", coord.NodeID, coordAddr)

	// Step 2: Connect directly to the Coordinator broker.
	coordCfg := cfg
	coordCfg.Addr = coordAddr
	coordConn, err := protocol.Dial(coordCfg)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator broker %s: %w", coordAddr, err)
	}
	defer coordConn.Close()

	// Step 3: Polling state machine.
	fmt.Printf("[PHASE 2] Polling coordinator state machine (%d samples at %v intervals)...\n\n", auditSamples, auditInterval)

	table := ui.NewTable("SAMPLE", "TIME", "STATE", "MEMBERS", "PROTOCOL", "TRANSITION")
	table.SetAlignment(ui.AlignRight, ui.AlignCenter, ui.AlignCenter, ui.AlignRight, ui.AlignLeft, ui.AlignLeft)

	type StateRecord struct {
		Index     int
		Timestamp time.Time
		State     string
		Members   int
		Protocol  string
	}

	records := make([]StateRecord, 0, auditSamples)
	prevState := ""
	transitions := 0
	rebalanceDetectedCount := 0
	var lastDescribedGroup *protocol.DescribedGroup

	for i := 1; i <= auditSamples; i++ {
		now := time.Now()
		group, descErr := protocol.DescribeGroup(coordConn, auditGroup)
		if descErr != nil {
			table.AddRow(
				fmt.Sprintf("%d", i),
				now.Format("15:04:05.000"),
				"[ERROR]",
				"-",
				"-",
				descErr.Error(),
			)
		} else {
			lastDescribedGroup = group
			currState := group.GroupState
			transitionDesc := "STEADY"
			if prevState != "" && prevState != currState {
				transitions++
				transitionDesc = fmt.Sprintf("%s -> %s", prevState, currState)
			} else if prevState == "" {
				transitionDesc = "INITIAL"
			}

			if currState == "PreparingRebalance" || currState == "CompletingRebalance" {
				rebalanceDetectedCount++
			}

			record := StateRecord{
				Index:     i,
				Timestamp: now,
				State:     currState,
				Members:   len(group.Members),
				Protocol:  group.ProtocolData,
			}
			records = append(records, record)

			stateBadge := ui.StatusBadge(currState)
			table.AddRow(
				fmt.Sprintf("%d", i),
				now.Format("15:04:05.000"),
				stateBadge,
				fmt.Sprintf("%d", len(group.Members)),
				group.ProtocolData,
				transitionDesc,
			)

			prevState = currState
		}

		if i < auditSamples {
			time.Sleep(auditInterval)
		}
	}

	fmt.Print(table.Render())
	fmt.Println()

	// Step 4: Diagnostic Assessment.
	fmt.Println("+============================================================================+")
	fmt.Println("| AUDIT ASSESSMENT & ROOT CAUSE ANALYSIS                                    |")
	fmt.Println("+----------------------------------------------------------------------------+")

	if transitions >= 2 || rebalanceDetectedCount >= 2 {
		fmt.Println("| STATUS: [CRITICAL] REBALANCE STORM DETECTED                                |")
		fmt.Printf("| - State transitions observed : %d\n", transitions)
		fmt.Printf("| - Rebalance states observed  : %d of %d samples\n", rebalanceDetectedCount, auditSamples)
		fmt.Println("|                                                                            |")
		fmt.Println("| ROOT CAUSE DIAGNOSIS:                                                      |")
		fmt.Println("| Consumer heartbeat failure or batch processing exceeding max.poll.interval |")
		fmt.Println("| caused the coordinator to mark a consumer dead and trigger rebalance.      |")
		fmt.Println("| Under eager assignors, all consumers revoked partitions. During rebalance, |")
		fmt.Println("| message backlog expanded, driving subsequent consumers into timeouts in an  |")
		fmt.Println("| infinite rebalance cascade.                                                |")
		fmt.Println("|                                                                            |")
		fmt.Println("| RECOMMENDED ACTION:                                                        |")
		fmt.Println("| 1. Run 'kafka-unwedge lag --group <group>' to find stalled partitions.     |")
		fmt.Println("| 2. Run 'kafka-unwedge fence' to administratively evict deadlocked members. |")
	} else if rebalanceDetectedCount == 1 {
		fmt.Println("| STATUS: [WARNING] TRANSIENT REBALANCE OBSERVED                             |")
		fmt.Printf("| - Observed 1 rebalance state across %d samples.\n", auditSamples)
		fmt.Println("| - Group may have completed a rolling deploy or single consumer joined/left.|")
	} else if prevState == "Stable" {
		fmt.Println("| STATUS: [HEALTHY] CONSUMER GROUP STABLE                                    |")
		fmt.Println("| - Group remained in Stable state throughout the observation window.        |")
		fmt.Printf("| - Active members: %d\n", len(records))
	} else {
		fmt.Printf("| STATUS: [ALERT] GROUP IN STATE '%s'\n", prevState)
	}
	fmt.Println("+============================================================================+")
	fmt.Println()

	// Step 5: Active Members Inventory.
	if lastDescribedGroup != nil && len(lastDescribedGroup.Members) > 0 {
		fmt.Println("ACTIVE GROUP MEMBERS INVENTORY:")
		memberTable := ui.NewTable("MEMBER ID", "CLIENT ID", "CLIENT HOST", "ASSIGNED PARTITIONS")
		memberTable.SetAlignment(ui.AlignLeft, ui.AlignLeft, ui.AlignLeft, ui.AlignLeft)

		for _, m := range lastDescribedGroup.Members {
			var partList []string
			for _, a := range m.Assignments {
				pStrs := make([]string, len(a.Partitions))
				for idx, p := range a.Partitions {
					pStrs[idx] = fmt.Sprintf("%d", p)
				}
				partList = append(partList, fmt.Sprintf("%s:[%s]", a.Topic, strings.Join(pStrs, ",")))
			}
			partDisplay := strings.Join(partList, " ")
			if partDisplay == "" {
				partDisplay = "(none)"
			}

			memberTable.AddRow(
				m.MemberId,
				m.ClientId,
				m.ClientHost,
				partDisplay,
			)
		}
		fmt.Print(memberTable.Render())
	}

	return nil
}
