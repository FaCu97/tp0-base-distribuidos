package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	nameFieldSize     = 30
	surnameFieldSize  = 30
	documentFieldSize = 8
	birthFieldSize    = 10
	numberFieldSize   = 4
	betFrameSize      = nameFieldSize + surnameFieldSize + documentFieldSize + birthFieldSize + numberFieldSize
	batchCountSize    = 2
	ackFrameSize      = 2
	maxBatchBytes     = 8 * 1024
)

type Bet struct {
	Nombre     string
	Apellido   string
	Documento  string
	Nacimiento string
	Numero     uint32
}

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopPeriod     time.Duration
	Bets           []Bet
	BatchMaxAmount int
}

// Client Entity that encapsulates how
type Client struct {
	config       ClientConfig
	conn         net.Conn
	mu           sync.Mutex
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:     config,
		shutdownCh: make(chan struct{}),
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	log.Infof("action: connect | result: success | client_id: %v", c.config.ID)
	return nil
}

func (c *Client) GracefulShutdown() {
	c.shutdownOnce.Do(func() {
		close(c.shutdownCh)
		c.closeClientSocket()
	})
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	if len(c.config.Bets) == 0 {
		log.Infof("action: loop_finished | result: success | client_id: %v | reason: no_bets", c.config.ID)
		return
	}

	if c.isShuttingDown() {
		log.Infof("action: loop_finished | result: success | client_id: %v | reason: shutdown", c.config.ID)
		return
	}

	batchSize := c.effectiveBatchSize()
	batches := splitBets(c.config.Bets, batchSize)

	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.closeClientSocket()

	conn := c.getConn()
	if conn == nil {
		log.Errorf("action: send_message | result: fail | client_id: %v | error: connection_closed", c.config.ID)
		return
	}

	for idx, batch := range batches {
		if c.isShuttingDown() {
			log.Infof("action: loop_finished | result: success | client_id: %v | reason: shutdown", c.config.ID)
			return
		}

		if err := c.sendBatchFrame(conn, batch); err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		ack, err := c.readAck(conn)
		if err != nil {
			if c.isShuttingDown() {
				log.Infof("action: receive_message | result: success | client_id: %v | reason: shutdown", c.config.ID)
				return
			}

			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		if ack != "OK" {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: invalid_ack", c.config.ID)
			return
		}

		for _, bet := range batch {
			log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %d", bet.Documento, bet.Numero)
		}

		if idx < len(batches)-1 && c.waitLoopPeriod() {
			log.Infof("action: loop_finished | result: success | client_id: %v | reason: shutdown", c.config.ID)
			return
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

func (c *Client) isShuttingDown() bool {
	select {
	case <-c.shutdownCh:
		return true
	default:
		return false
	}
}

func (c *Client) waitLoopPeriod() bool {
	timer := time.NewTimer(c.config.LoopPeriod)
	defer timer.Stop()

	select {
	case <-timer.C:
		return false
	case <-c.shutdownCh:
		return true
	}
}

func (c *Client) getConn() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

func (c *Client) closeClientSocket() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn == nil {
		return
	}

	if err := conn.Close(); err != nil {
		log.Errorf("action: close_resource | result: fail | resource: client_socket | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: close_resource | result: success | resource: client_socket | client_id: %v", c.config.ID)
}

func (c *Client) sendBatchFrame(conn net.Conn, batch []Bet) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	if len(batch) > 0xFFFF {
		return fmt.Errorf("batch size exceeds uint16 limit")
	}

	frame, err := buildBatchFrame(batch)
	if err != nil {
		return err
	}

	totalWritten := 0
	for totalWritten < len(frame) {
		n, writeErr := conn.Write(frame[totalWritten:])
		if writeErr != nil {
			return writeErr
		}
		if n == 0 {
			return fmt.Errorf("short write detected")
		}
		totalWritten += n
	}

	return nil
}

func buildBatchFrame(batch []Bet) ([]byte, error) {
	payloadSize := len(batch) * betFrameSize
	totalSize := batchCountSize + payloadSize
	if totalSize > maxBatchBytes {
		return nil, fmt.Errorf("batch frame exceeds %d bytes", maxBatchBytes)
	}

	frame := make([]byte, totalSize)
	binary.BigEndian.PutUint16(frame[:batchCountSize], uint16(len(batch)))
	offset := batchCountSize

	for _, bet := range batch {
		betFrame, err := buildBetFrame(bet)
		if err != nil {
			return nil, err
		}

		copy(frame[offset:offset+betFrameSize], betFrame)
		offset += betFrameSize
	}

	return frame, nil
}

func splitBets(bets []Bet, batchSize int) [][]Bet {
	if batchSize <= 0 {
		batchSize = 1
	}

	batches := make([][]Bet, 0, (len(bets)+batchSize-1)/batchSize)
	for start := 0; start < len(bets); start += batchSize {
		end := start + batchSize
		if end > len(bets) {
			end = len(bets)
		}
		batches = append(batches, bets[start:end])
	}

	return batches
}

func (c *Client) effectiveBatchSize() int {
	maxByBytes := (maxBatchBytes - batchCountSize) / betFrameSize
	if maxByBytes <= 0 {
		return 1
	}

	configured := c.config.BatchMaxAmount
	if configured <= 0 {
		configured = maxByBytes
	}

	if configured > maxByBytes {
		configured = maxByBytes
	}

	return configured
}

func (c *Client) readAck(conn net.Conn) (string, error) {
	ack := make([]byte, ackFrameSize)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return "", err
	}

	return string(ack), nil
}

func buildBetFrame(bet Bet) ([]byte, error) {
	if len(bet.Documento) > documentFieldSize {
		return nil, fmt.Errorf("documento exceeds %d bytes", documentFieldSize)
	}

	if len(bet.Nacimiento) != birthFieldSize {
		return nil, fmt.Errorf("nacimiento must have format AAAA-MM-DD")
	}

	frame := make([]byte, betFrameSize)
	offset := 0

	copy(frame[offset:offset+nameFieldSize], formatRightPaddedField(bet.Nombre, nameFieldSize))
	offset += nameFieldSize

	copy(frame[offset:offset+surnameFieldSize], formatRightPaddedField(bet.Apellido, surnameFieldSize))
	offset += surnameFieldSize

	copy(frame[offset:offset+documentFieldSize], formatLeftZeroPaddedField(bet.Documento, documentFieldSize))
	offset += documentFieldSize

	copy(frame[offset:offset+birthFieldSize], []byte(bet.Nacimiento))
	offset += birthFieldSize

	binary.BigEndian.PutUint32(frame[offset:offset+numberFieldSize], bet.Numero)

	return frame, nil
}

func formatRightPaddedField(value string, width int) []byte {
	trimmed := strings.TrimSpace(value)
	field := make([]byte, width)
	for i := range field {
		field[i] = ' '
	}

	raw := []byte(trimmed)
	if len(raw) > width {
		raw = raw[:width]
	}

	copy(field, raw)
	return field
}

func formatLeftZeroPaddedField(value string, width int) []byte {
	trimmed := strings.TrimSpace(value)
	raw := []byte(trimmed)
	if len(raw) > width {
		raw = raw[len(raw)-width:]
	}

	field := make([]byte, width)
	for i := range field {
		field[i] = '0'
	}

	copy(field[width-len(raw):], raw)
	return field
}
