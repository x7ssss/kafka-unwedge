package cmd

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/x7ssss/kafka-unwedge/pkg/protocol"
)

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "kafka-unwedge",
	Short: "Zero-dependency Kafka rebalance storm auditor, lag velocity analyzer, and zombie fencer",
	Long: `kafka-unwedge is a pure Go CLI designed to audit Kafka consumer group rebalance storms,
measure differential lag velocity (dLag/dt), and dynamically fence zombie members using raw
Kafka wire protocol without restarting consumer pods or depending on external Kafka frameworks.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// createConnectionConfig builds a protocol.Config from standard CLI flags.
func createConnectionConfig(broker string, useTLS bool, timeout time.Duration) protocol.Config {
	var tlsConf *tls.Config
	if useTLS {
		tlsConf = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	return protocol.Config{
		Addr:      broker,
		UseTLS:    useTLS,
		TLSConfig: tlsConf,
		Timeout:   timeout,
		ClientID:  "kafka-unwedge",
	}
}
