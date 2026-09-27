# 📨 kafka-unwedge

A zero-dependency Go CLI designed to audit Kafka consumer group rebalance storms, detect partition lag velocity, and dynamically fence zombie members using raw Kafka wire protocol without restarting consumer pods.

---

## ⚡ Key Features

1. 📦 **Zero External Kafka Frameworks Invariant**:
   - 🔧 Zero JVM wrappers, no `librdkafka` / CGo dependencies, no `Sarama` or heavyweight SDKs.
   - 🚀 Built exclusively on pure Go standard library (`net`, `encoding/binary`, `crypto/tls`, `bufio`, `bytes`, `time`) and `github.com/spf13/cobra v1.8.1`.
   - 📦 Ultra-compact compiled binary size under 10MB statically linked (`CGO_ENABLED=0`).
   - 🛡️ Strictly NO em dashes anywhere in comments, code, or output strings.

2. 🛡️ **Rebalance Storm Breaker**:
   - 📨 When consumers perform heavy batch processing exceeding `max.poll.interval.ms`, client heartbeats cease.
   - ⚠️ The coordinator marks the consumer dead and triggers `PreparingRebalance`. Under eager assignors, all consumers yield partitions and backlog accumulates.
   - 🔄 The next consumer times out on the larger backlog, repeating the loop infinitely.
   - 🔍 `kafka-unwedge audit` samples coordinator state transitions to detect rebalance storms and flapping.

3. 🔍 **Differential Lag Analyzer (dLag/dt)**:
   - 📊 Measures commit velocity and lag acceleration across temporal windows (default 5s).
   - ⏱️ Samples committed offsets (OffsetFetch API Key 9 v1) and Log End Offsets (ListOffsets API Key 2 v1).
   - 🩺 Classifies partition health:
     - ⚠️ `STALLED`: Delta Commit == 0 and Lag > 0.
     - 📉 `FALLING_BEHIND`: Delta Lag > 0 (ingestion rate outpaces consumption).
     - ✅ `HEALTHY`: Lag == 0 or consumer is actively draining.

4. 🔧 **Administrative Zombie Fencer**:
   - ⚡ Issues `LeaveGroup` (API Key 13 v3) with `member_id` or `group_instance_id` directly to the coordinator.
   - 🚀 Evicts stuck or deadlocked workers broker-side, prompting immediate partition reassignment without requiring pod restarts or fleet redeployment.

---

## 📨 Wire Protocol Architecture

`kafka-unwedge` implements raw binary Kafka wire protocol framing:
- 📨 **Request Header v1**:
  - 🔍 `api_key` (int16)
  - 🔍 `api_version` (int16)
  - 🔍 `correlation_id` (int32)
  - 🔍 `client_id` (string: 2-byte length + UTF-8 bytes)
- 📨 **Response Header v0**:
  - 🔍 `correlation_id` (int32)
- 📦 **Message Framing**:
  - ⚡ 4-byte big-endian length prefix framing every packet over raw TCP/TLS sockets.

### 📋 Protocol Operations Implemented:
| API Name | API Key | Version | Purpose |
| :--- | :---: | :---: | :--- |
| `FindCoordinator` | 10 | v1 | Locate group coordinator broker (KeyType=0) |
| `DescribeGroups` | 15 | v0 | Extract group state and consumer member assignments |
| `LeaveGroup` | 13 | v3 | Administrative eviction of dynamic and static members |
| `OffsetFetch` | 9 | v1 | Fetch consumer group committed partition offsets |
| `ListOffsets` | 2 | v1 | Fetch partition Log End Offsets (LEO, timestamp=-1) |
| `Metadata` | 3 | v0 | Query partition leadership for ListOffsets routing |

---

## 📦 Installation & Compilation

### 🔧 Local Build
```bash
go build ./cmd/kafka-unwedge
```

### ⚡ Static Stripped Build (Under 10MB)
```bash
# Linux/macOS
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o kafka-unwedge ./cmd/kafka-unwedge

# PowerShell (Windows)
$env:CGO_ENABLED="0"; go build -trimpath -ldflags="-s -w" -o kafka-unwedge.exe ./cmd/kafka-unwedge
```

### 🚀 Cross-Compilation
Use the provided scripts to build static binaries for Linux, macOS, and Windows into `dist/`:
```bash
# PowerShell
.\build.ps1

# Make
make cross-build
```

---

## 🔧 CLI Usage

### 1. 🔍 Audit Coordinator State Machine & Flapping
Polls the group coordinator state machine in real time across samples to detect rebalance loops:
```bash
kafka-unwedge audit --broker localhost:9092 --group my-consumer-group --samples 10 --interval 1s
```
Flags:
- 🔧 `--broker` (required): Kafka bootstrap broker address (`host:port`).
- 🔧 `--group` (required): Target consumer group ID.
- 🔧 `--samples` (default: 10): Number of coordinator state samples to poll.
- ⏱️ `--interval` (default: 1s): Polling interval between samples.
- 🛡️ `--tls`: Enable TLS 1.2+ encryption.

### 2. ⚡ Differential Lag Analyzer (dLag/dt)
Evaluates commit velocity and lag expansion across a temporal sampling window:
```bash
kafka-unwedge lag --broker localhost:9092 --group my-consumer-group --window 5s
```
Flags:
- 🔧 `--broker` (required): Kafka bootstrap broker address (`host:port`).
- 🔧 `--group` (required): Target consumer group ID.
- ⏱️ `--window` (default: 5s): Sampling duration between t0 and t1.
- 🛡️ `--tls`: Enable TLS 1.2+ encryption.

### 3. 🛡️ Zombie Consumer Fencing
Administratively evicts a deadlocked consumer member broker-side without restarting pods:
```bash
# Evict dynamic member by member ID
kafka-unwedge fence --broker localhost:9092 --group my-consumer-group --member-id consumer-1-47a3e782

# Evict static member by group.instance.id
kafka-unwedge fence --broker localhost:9092 --group my-consumer-group --group-instance-id pod-worker-0
```
Flags:
- 🔧 `--broker` (required): Kafka bootstrap broker address (`host:port`).
- 🔧 `--group` (required): Target consumer group ID.
- 🔍 `--member-id`: Dynamic member ID to evict.
- 🔍 `--group-instance-id`: Static member `group.instance.id` to evict.
- 🛡️ `--tls`: Enable TLS 1.2+ encryption.

---

## 🩺 Verification & Tests

Run all unit tests with race detection:
```bash
go test -v ./...
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2026 x7ssss
