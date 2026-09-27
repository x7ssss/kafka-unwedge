package cmd

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/x7ssss/kafka-unwedge/pkg/protocol"
	"github.com/x7ssss/kafka-unwedge/pkg/ui"
)

var (
	fenceBroker          string
	fenceGroup           string
	fenceMemberID        string
	fenceGroupInstanceID string
	fenceTLS             bool
)

var fenceCmd = &cobra.Command{
	Use:   "fence",
	Short: "Administratively evict and fence zombie consumer members via raw Kafka wire protocol",
	Long: `Directly issues administrative LeaveGroup (API Key 13 v3) requests to the consumer
group coordinator to immediately prune deadlocked, unresponsive, or wedged members.
This triggers coordinator-side partition reassignment without requiring consumer pod restarts.`,
	RunE: runFence,
}

func init() {
	fenceCmd.Flags().StringVar(&fenceBroker, "broker", "", "Kafka bootstrap broker address (host:port)")
	fenceCmd.Flags().StringVar(&fenceGroup, "group", "", "Kafka consumer group ID")
	fenceCmd.Flags().StringVar(&fenceMemberID, "member-id", "", "Dynamic Kafka consumer member ID to evict")
	fenceCmd.Flags().StringVar(&fenceGroupInstanceID, "group-instance-id", "", "Static member group.instance.id to evict")
	fenceCmd.Flags().BoolVar(&fenceTLS, "tls", false, "Enable TLS connection to Kafka broker")

	_ = fenceCmd.MarkFlagRequired("broker")
	_ = fenceCmd.MarkFlagRequired("group")

	RootCmd.AddCommand(fenceCmd)
}

func runFence(cmd *cobra.Command, args []string) error {
	if fenceMemberID == "" && fenceGroupInstanceID == "" {
		return errors.New("must specify at least one eviction target: --member-id or --group-instance-id")
	}

	tlsStr := "DISABLED"
	if fenceTLS {
		tlsStr = "ENABLED (TLS 1.2+)"
	}

	targetDesc := fenceMemberID
	if targetDesc == "" {
		targetDesc = "(none)"
	}
	instanceDesc := fenceGroupInstanceID
	if instanceDesc == "" {
		instanceDesc = "(none)"
	}

	bannerMeta := [][2]string{
		{"OPERATION", "ZOMBIE CONSUMER FENCING"},
		{"BOOTSTRAP BROKER", fenceBroker},
		{"TARGET GROUP", fenceGroup},
		{"TARGET MEMBER ID", targetDesc},
		{"TARGET INSTANCE ID", instanceDesc},
		{"SECURITY", tlsStr},
	}
	fmt.Print(ui.Banner("KAFKA-UNWEDGE :: ADMINISTRATIVE ZOMBIE FENCER", bannerMeta))

	cfg := createConnectionConfig(fenceBroker, fenceTLS, 10*time.Second)

	// Step 1: Discover Group Coordinator.
	fmt.Printf("[PHASE 1] Resolving group coordinator for group '%s'...\n", fenceGroup)
	bootstrapConn, err := protocol.Dial(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to bootstrap broker: %w", err)
	}
	defer bootstrapConn.Close()

	coord, err := protocol.FindCoordinator(bootstrapConn, fenceGroup)
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

	// Step 3: Issue LeaveGroup v3 Request.
	var instPtr *string
	if fenceGroupInstanceID != "" {
		instPtr = &fenceGroupInstanceID
	}

	identity := protocol.LeaveMemberIdentity{
		MemberId:        fenceMemberID,
		GroupInstanceId: instPtr,
	}

	fmt.Printf("[PHASE 2] Issuing administrative LeaveGroup (API Key 13 v3) to coordinator...\n")
	t0 := time.Now()
	resp, err := protocol.LeaveGroup(coordConn, fenceGroup, []protocol.LeaveMemberIdentity{identity})
	elapsed := time.Since(t0)

	if err != nil {
		fmt.Printf("\n[FAILURE] Coordinator rejected eviction request: %v\n", err)
		return err
	}

	fmt.Printf("[RESPONSE] Received coordinator acknowledgment in %v (ThrottleTime: %d ms)\n\n",
		elapsed.Round(time.Millisecond), resp.ThrottleTimeMs)

	// Step 4: Display Eviction Results.
	table := ui.NewTable("MEMBER ID", "INSTANCE ID", "ERROR CODE", "RESULT")
	table.SetAlignment(ui.AlignLeft, ui.AlignLeft, ui.AlignCenter, ui.AlignLeft)

	allSuccess := true
	for _, m := range resp.Members {
		instStr := "(none)"
		if m.GroupInstanceId != nil {
			instStr = *m.GroupInstanceId
		}

		midStr := m.MemberId
		if midStr == "" {
			midStr = "(unspecified)"
		}

		resultStr := "SUCCESS (FENCED & EVICTED)"
		if m.ErrorCode != protocol.ErrNone {
			allSuccess = false
			resultStr = fmt.Sprintf("FAILED: %s", protocol.ErrorCodeText(m.ErrorCode))
		}

		table.AddRow(
			midStr,
			instStr,
			fmt.Sprintf("%d", m.ErrorCode),
			resultStr,
		)
	}

	fmt.Print(table.Render())
	fmt.Println()

	fmt.Println("+============================================================================+")
	if allSuccess {
		fmt.Println("| EVICTION COMPLETE: ZOMBIE MEMBER REMOVED BROKER-SIDE                       |")
		fmt.Println("+----------------------------------------------------------------------------+")
		fmt.Println("| The coordinator has unassigned this member and triggered a clean rebalance.|")
		fmt.Println("| Remaining healthy consumer pods will immediately claim abandoned partitions|")
		fmt.Println("| without requiring any pod restarts, rolling deployments, or disruptions.   |")
	} else {
		fmt.Println("| EVICTION NOTICE: COORDINATOR RETURNED AN ERROR CODE                        |")
		fmt.Println("+----------------------------------------------------------------------------+")
		fmt.Println("| Review the error code returned above. The member may have already left the |")
		fmt.Println("| group or the instance ID was not recognized by the coordinator.            |")
	}
	fmt.Println("+============================================================================+")

	return nil
}
