package protocol

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultClientID sent in Kafka request headers.
const DefaultClientID = "kafka-unwedge"

// Connection encapsulates a raw TCP or TLS connection to a Kafka broker.
type Connection struct {
	conn          net.Conn
	clientID      string
	correlationID int32
	mu            sync.Mutex
	timeout       time.Duration
}

// Config specifies connection settings.
type Config struct {
	Addr       string
	UseTLS     bool
	TLSConfig  *tls.Config
	Timeout    time.Duration
	ClientID   string
}

// Dial connects to a broker via plain TCP or TLS.
func Dial(cfg Config) (*Connection, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = DefaultClientID
	}

	var rawConn net.Conn
	var err error

	if cfg.UseTLS {
		tlsConf := cfg.TLSConfig
		if tlsConf == nil {
			tlsConf = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}
		rawConn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", cfg.Addr, tlsConf)
	} else {
		rawConn, err = net.DialTimeout("tcp", cfg.Addr, timeout)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %w", cfg.Addr, err)
	}

	return &Connection{
		conn:     rawConn,
		clientID: clientID,
		timeout:  timeout,
	}, nil
}

// Close closes the underlying network connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SendRequest frames and transmits a Kafka request using Header v1, then reads the Header v0 response.
func (c *Connection) SendRequest(apiKey int16, apiVersion int16, body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	corrID := atomic.AddInt32(&c.correlationID, 1)

	// Build Request Header v1:
	// api_key: int16
	// api_version: int16
	// correlation_id: int32
	// client_id: string (int16 len + bytes)
	headerWriter := NewWriter()
	headerWriter.WriteInt16(apiKey)
	headerWriter.WriteInt16(apiVersion)
	headerWriter.WriteInt32(corrID)
	headerWriter.WriteString(c.clientID)

	headerBytes := headerWriter.Bytes()
	totalPayloadLen := len(headerBytes) + len(body)

	// Message frame: 4-byte length prefix followed by header and body.
	frameWriter := NewWriter()
	frameWriter.WriteInt32(int32(totalPayloadLen))
	frameWriter.WriteRaw(headerBytes)
	frameWriter.WriteRaw(body)

	if c.timeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	}

	// Transmit request frame.
	if _, err := c.conn.Write(frameWriter.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write request frame: %w", err)
	}

	// Read 4-byte response length prefix.
	var lenBuf [4]byte
	if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("failed to read response size prefix: %w", err)
	}

	respLen := binary.BigEndian.Uint32(lenBuf[:])
	if respLen < 4 {
		return nil, fmt.Errorf("invalid response length: %d (must be at least 4 bytes for correlation id)", respLen)
	}

	// Read full response packet.
	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respBuf); err != nil {
		return nil, fmt.Errorf("failed to read full response payload of %d bytes: %w", respLen, err)
	}

	// Reset deadline.
	if c.timeout > 0 {
		_ = c.conn.SetDeadline(time.Time{})
	}

	// Parse Response Header v0: correlation_id (int32).
	r := NewReader(respBuf)
	returnedCorrID, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to parse response correlation id: %w", err)
	}

	if returnedCorrID != corrID {
		return nil, fmt.Errorf("correlation id mismatch: sent %d, received %d", corrID, returnedCorrID)
	}

	// Return remaining response body.
	bodyBytes := respBuf[4:]
	return bodyBytes, nil
}
