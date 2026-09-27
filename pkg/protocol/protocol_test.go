package protocol

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestMockBrokerWireFraming(t *testing.T) {
	// Setup an in-memory TCP listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Mock server goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErrChan <- acceptErr
			return
		}
		defer conn.Close()

		// Read 4-byte size
		var sizeBuf [4]byte
		if _, readErr := io.ReadFull(conn, sizeBuf[:]); readErr != nil {
			serverErrChan <- readErr
			return
		}
		size := binary.BigEndian.Uint32(sizeBuf[:])

		reqPayload := make([]byte, size)
		if _, readErr := io.ReadFull(conn, reqPayload); readErr != nil {
			serverErrChan <- readErr
			return
		}

		// Verify Request Header v1
		r := NewReader(reqPayload)
		apiKey, _ := r.ReadInt16()
		apiVer, _ := r.ReadInt16()
		corrID, _ := r.ReadInt32()
		clientID, _ := r.ReadString()

		if apiKey != ApiKeyFindCoordinator || apiVer != 1 || clientID != "kafka-unwedge" {
			t.Errorf("Header mismatch: apiKey=%d, apiVer=%d, clientID=%s", apiKey, apiVer, clientID)
		}

		// Send mock FindCoordinator Response v1:
		// Response Header v0: correlation_id (int32)
		// Body: ThrottleTimeMs(int32), ErrorCode(int16), ErrorMessage(nullable string), NodeId(int32), Host(string), Port(int32)
		respWriter := NewWriter()
		respWriter.WriteInt32(corrID) // Response Header v0
		respWriter.WriteInt32(0)      // ThrottleTimeMs
		respWriter.WriteInt16(0)      // ErrorCode: NONE
		respWriter.WriteNullableString(nil)
		respWriter.WriteInt32(101)                 // NodeID
		respWriter.WriteString("kafka-broker-101") // Host
		respWriter.WriteInt32(9092)                // Port

		respBytes := respWriter.Bytes()
		var frameBuf [4]byte
		binary.BigEndian.PutUint32(frameBuf[:], uint32(len(respBytes)))

		if _, writeErr := conn.Write(frameBuf[:]); writeErr != nil {
			serverErrChan <- writeErr
			return
		}
		if _, writeErr := conn.Write(respBytes); writeErr != nil {
			serverErrChan <- writeErr
			return
		}

		serverErrChan <- nil
	}()

	// Client connection
	c, err := Dial(Config{
		Addr:     addr,
		Timeout:  3 * time.Second,
		ClientID: "kafka-unwedge",
	})
	if err != nil {
		t.Fatalf("Failed to dial test broker: %v", err)
	}
	defer c.Close()

	coord, err := FindCoordinator(c, "payment-processors")
	if err != nil {
		t.Fatalf("FindCoordinator failed: %v", err)
	}

	if coord.NodeID != 101 {
		t.Errorf("Expected coordinator NodeID 101, got %d", coord.NodeID)
	}
	if coord.Host != "kafka-broker-101" || coord.Port != 9092 {
		t.Errorf("Expected host kafka-broker-101:9092, got %s:%d", coord.Host, coord.Port)
	}
	if coord.Addr() != "kafka-broker-101:9092" {
		t.Errorf("Expected Addr() = kafka-broker-101:9092, got %s", coord.Addr())
	}

	if sErr := <-serverErrChan; sErr != nil {
		t.Fatalf("Server error: %v", sErr)
	}
}

func TestLeaveGroupWireCodec(t *testing.T) {
	// Setup listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	serverErrChan := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErrChan <- acceptErr
			return
		}
		defer conn.Close()

		var sizeBuf [4]byte
		if _, readErr := io.ReadFull(conn, sizeBuf[:]); readErr != nil {
			serverErrChan <- readErr
			return
		}
		size := binary.BigEndian.Uint32(sizeBuf[:])
		reqPayload := make([]byte, size)
		if _, readErr := io.ReadFull(conn, reqPayload); readErr != nil {
			serverErrChan <- readErr
			return
		}

		r := NewReader(reqPayload)
		apiKey, _ := r.ReadInt16()
		apiVer, _ := r.ReadInt16()
		corrID, _ := r.ReadInt32()
		_, _ = r.ReadString() // client_id

		if apiKey != ApiKeyLeaveGroup || apiVer != 3 {
			t.Errorf("Unexpected API key %d or version %d", apiKey, apiVer)
		}

		groupID, _ := r.ReadString()
		if groupID != "order-consumers" {
			t.Errorf("Expected group 'order-consumers', got '%s'", groupID)
		}

		memCount, _ := r.ReadArrayLen()
		if memCount != 1 {
			t.Errorf("Expected 1 member, got %d", memCount)
		}
		mid, _ := r.ReadString()
		instID, _ := r.ReadNullableString()

		if mid != "client-42" {
			t.Errorf("Expected memberId 'client-42', got '%s'", mid)
		}
		if instID == nil || *instID != "pod-order-0" {
			t.Errorf("Expected instanceId 'pod-order-0', got %v", instID)
		}

		// Respond with LeaveGroupResponse v3
		respWriter := NewWriter()
		respWriter.WriteInt32(corrID) // header v0
		respWriter.WriteInt32(0)      // ThrottleTimeMs
		respWriter.WriteInt16(0)      // ErrorCode: NONE
		respWriter.WriteArrayLen(1)   // Members count
		respWriter.WriteString("client-42")
		respWriter.WriteNullableString(instID)
		respWriter.WriteInt16(0) // ErrorCode: NONE

		respBytes := respWriter.Bytes()
		var frameBuf [4]byte
		binary.BigEndian.PutUint32(frameBuf[:], uint32(len(respBytes)))
		_, _ = conn.Write(frameBuf[:])
		_, _ = conn.Write(respBytes)

		serverErrChan <- nil
	}()

	c, err := Dial(Config{
		Addr:     listener.Addr().String(),
		Timeout:  3 * time.Second,
		ClientID: "kafka-unwedge",
	})
	if err != nil {
		t.Fatalf("Failed to dial mock: %v", err)
	}
	defer c.Close()

	inst := "pod-order-0"
	resp, err := LeaveGroup(c, "order-consumers", []LeaveMemberIdentity{
		{MemberId: "client-42", GroupInstanceId: &inst},
	})
	if err != nil {
		t.Fatalf("LeaveGroup failed: %v", err)
	}

	if resp.ErrorCode != ErrNone {
		t.Errorf("Expected ErrorCode 0, got %d", resp.ErrorCode)
	}
	if len(resp.Members) != 1 {
		t.Fatalf("Expected 1 member response, got %d", len(resp.Members))
	}
	if resp.Members[0].MemberId != "client-42" {
		t.Errorf("Expected MemberId 'client-42', got '%s'", resp.Members[0].MemberId)
	}
	if resp.Members[0].GroupInstanceId == nil || *resp.Members[0].GroupInstanceId != "pod-order-0" {
		t.Errorf("Expected GroupInstanceId 'pod-order-0', got %v", resp.Members[0].GroupInstanceId)
	}

	if sErr := <-serverErrChan; sErr != nil {
		t.Fatalf("Server error: %v", sErr)
	}
}

func TestConsumerAssignmentCodec(t *testing.T) {
	// Build mock consumer protocol assignment
	w := NewWriter()
	w.WriteInt16(0)           // version 0
	w.WriteArrayLen(2)        // 2 topics
	w.WriteString("orders")   // topic 1
	w.WriteArrayLen(3)        // partitions: 0, 1, 2
	w.WriteInt32(0)
	w.WriteInt32(1)
	w.WriteInt32(2)
	w.WriteString("invoices") // topic 2
	w.WriteArrayLen(1)        // partition 0
	w.WriteInt32(0)
	w.WriteBytes([]byte("user-metadata")) // user data

	data := w.Bytes()
	assignments, err := parseConsumerProtocolAssignment(data)
	if err != nil {
		t.Fatalf("parseConsumerProtocolAssignment failed: %v", err)
	}

	if len(assignments) != 2 {
		t.Fatalf("Expected 2 topic assignments, got %d", len(assignments))
	}
	if assignments[0].Topic != "orders" || len(assignments[0].Partitions) != 3 {
		t.Errorf("Unexpected orders assignment: %+v", assignments[0])
	}
	if assignments[1].Topic != "invoices" || len(assignments[1].Partitions) != 1 {
		t.Errorf("Unexpected invoices assignment: %+v", assignments[1])
	}
}
