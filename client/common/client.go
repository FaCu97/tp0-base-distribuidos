package common

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
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
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		if c.isShuttingDown() {
			log.Infof("action: loop_finished | result: success | client_id: %v | reason: shutdown", c.config.ID)
			return
		}

		// Create the connection the server in every loop iteration. Send an
		if err := c.createClientSocket(); err != nil {
			return
		}

		// TODO: Modify the send to avoid short-write
		conn := c.getConn()
		if conn == nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: connection_closed", c.config.ID)
			return
		}

		if _, err := fmt.Fprintf(
			conn,
			"[CLIENT %v] Message N°%v\n",
			c.config.ID,
			msgID,
		); err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.closeClientSocket()
			return
		}

		msg, err := bufio.NewReader(conn).ReadString('\n')
		c.closeClientSocket()

		if err != nil {
			if c.isShuttingDown() {
				log.Infof("action: receive_message | result: success | client_id: %v | reason: shutdown", c.config.ID)
				return
			}

			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
			c.config.ID,
			msg,
		)

		// Wait a time between sending one message and the next one
		if c.waitLoopPeriod() {
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
