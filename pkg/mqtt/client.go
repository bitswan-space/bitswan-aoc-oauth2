package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
)

// Client represents an MQTT client for dynamic group loading
type Client struct {
	client               mqtt.Client
	broker               string
	port                 int
	username             string
	password             string
	topic                string
	allowedGroups        []string
	allowedGroupsMutex   sync.RWMutex
	onGroupsUpdate       func([]string)
	reconnectInterval    time.Duration
	maxReconnectAttempts int
}

// GroupUpdate represents the structure of MQTT messages for group updates
type GroupUpdate struct {
	AllowedGroups []string `json:"allowed_groups"`
	Timestamp     string   `json:"timestamp,omitempty"`
}

// NewClient creates a new MQTT client
func NewClient(broker string, port int, username, password, topic string) *Client {
	return &Client{
		broker:               broker,
		port:                 port,
		username:             username,
		password:             password,
		topic:                topic,
		reconnectInterval:    5 * time.Second,
		maxReconnectAttempts: 10,
	}
}

// SetOnGroupsUpdate sets the callback function for when groups are updated
func (c *Client) SetOnGroupsUpdate(callback func([]string)) {
	c.onGroupsUpdate = callback
}

// Connect establishes connection to the MQTT broker
func (c *Client) Connect() error {
	if c.broker == "" || c.topic == "" {
		logger.Printf("MQTT not configured, skipping connection")
		return nil
	}

	brokerURL := fmt.Sprintf("tcp://%s:%d", c.broker, c.port)
	logger.Printf("Connecting to MQTT broker: %s", brokerURL)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(fmt.Sprintf("oauth2-proxy-%d", time.Now().Unix()))

	if c.username != "" {
		opts.SetUsername(c.username)
		opts.SetPassword(c.password)
	}

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(c.reconnectInterval)
	opts.SetMaxReconnectInterval(c.reconnectInterval * 2)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)
	opts.SetResumeSubs(true)

	// Set connection callbacks
	opts.SetOnConnectHandler(c.onConnect)
	opts.SetConnectionLostHandler(c.onConnectionLost)
	opts.SetDefaultPublishHandler(c.onMessageReceived)

	c.client = mqtt.NewClient(opts)

	// Connect to broker
	token := c.client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %v", token.Error())
	}

	logger.Printf("Successfully connected to MQTT broker")
	return nil
}

// onConnect handles successful connection to MQTT broker
func (c *Client) onConnect(client mqtt.Client) {
	logger.Printf("Connected to MQTT broker, subscribing to topic: %s", c.topic)

	// Subscribe to the groups topic
	token := client.Subscribe(c.topic, 1, nil)
	if token.Wait() && token.Error() != nil {
		logger.Errorf("Failed to subscribe to topic %s: %v", c.topic, token.Error())
		return
	}

	logger.Printf("Successfully subscribed to topic: %s", c.topic)
}

// onConnectionLost handles connection loss
func (c *Client) onConnectionLost(_ mqtt.Client, err error) {
	logger.Errorf("MQTT connection lost: %v", err)
}

// onMessageReceived handles incoming MQTT messages
func (c *Client) onMessageReceived(_ mqtt.Client, msg mqtt.Message) {
	logger.Printf("Received MQTT message on topic %s: %s", msg.Topic(), string(msg.Payload()))

	var groupUpdate GroupUpdate
	if err := json.Unmarshal(msg.Payload(), &groupUpdate); err != nil {
		logger.Errorf("Failed to parse MQTT message: %v", err)
		return
	}

	// Update allowed groups
	c.updateAllowedGroups(groupUpdate.AllowedGroups)
}

// updateAllowedGroups updates the allowed groups and calls the callback
func (c *Client) updateAllowedGroups(groups []string) {
	c.allowedGroupsMutex.Lock()
	c.allowedGroups = groups
	c.allowedGroupsMutex.Unlock()

	logger.Printf("Updated allowed groups via MQTT: %v", groups)

	// Call the callback if set
	if c.onGroupsUpdate != nil {
		c.onGroupsUpdate(groups)
	}
}

// GetAllowedGroups returns the current allowed groups
func (c *Client) GetAllowedGroups() []string {
	c.allowedGroupsMutex.RLock()
	defer c.allowedGroupsMutex.RUnlock()

	// Return a copy to prevent external modification
	groups := make([]string, len(c.allowedGroups))
	copy(groups, c.allowedGroups)
	return groups
}

// Disconnect disconnects from the MQTT broker
func (c *Client) Disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
		logger.Printf("Disconnected from MQTT broker")
	}
}

// IsConnected returns true if connected to MQTT broker
func (c *Client) IsConnected() bool {
	return c.client != nil && c.client.IsConnected()
}