package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zahmadsaleem/mcp-go/mcp"
)

const (
	defaultToolResponseSizeLimit = 1 * 1024         // 1 MB
	defaultSSEReadTimeout        = 30 * time.Second // when the connection is inactive for this duration, the client will close the connection, the server might have gone away
	defaultResponseTimeout       = 30 * time.Second
	defaultSSEMaxLifetime        = 10 * time.Minute // entire duration of the mcp connection

)

type connectionState int

const (
	unInitialized connectionState = iota
	started
	initialized
	closed
)

var (
	ErrResponseTimeout  = errors.New("mcpclient: response timed out")
	ErrResponseTooLarge = errors.New("mcpclient: response too large")
)

// SSEMCPClient implements the MCPClient interface using Server-Sent Events (SSE).
// It maintains a persistent HTTP connection to receive server-pushed events
// while sending requests over regular HTTP POST calls. The client handles
// automatic reconnection and message routing between requests and responses.
type SSEMCPClient struct {
	baseURL               *url.URL
	endpoint              *url.URL
	httpClient            *http.Client
	requestID             atomic.Int64
	responses             map[int64]chan RPCResponse
	mu                    sync.RWMutex
	done                  chan struct{}
	initialized           bool
	notifications         []func(mcp.JSONRPCNotification)
	notifyMu              sync.RWMutex
	endpointChan          chan struct{}
	capabilities          mcp.ServerCapabilities
	headers               map[string]string
	sseReadTimeout        time.Duration
	toolResponseSizeLimit int
	responseTimeout       time.Duration
	maxSSELifetime        time.Duration
	state                 atomic.Int32
}

type ClientOption func(*SSEMCPClient)

func WithHeaders(headers map[string]string) ClientOption {
	return func(sc *SSEMCPClient) {
		sc.headers = headers
	}
}

func WithSSEReadTimeout(timeout time.Duration) ClientOption {
	return func(sc *SSEMCPClient) {
		sc.sseReadTimeout = timeout
	}
}

func WithToolResponseSizeLimit(sizeLimit int) ClientOption {
	return func(sc *SSEMCPClient) {
		if sizeLimit <= 0 {
			sizeLimit = defaultToolResponseSizeLimit
		}
		sc.toolResponseSizeLimit = sizeLimit
	}
}

func WithResponseTimeout(timeout time.Duration) ClientOption {
	return func(sc *SSEMCPClient) {
		if timeout <= 0 {
			timeout = defaultResponseTimeout
		}
		sc.responseTimeout = timeout
	}
}

func WithMaxSSELifetime(lifetime time.Duration) ClientOption {
	return func(sc *SSEMCPClient) {
		if lifetime <= 0 {
			lifetime = defaultSSEMaxLifetime
		}
		sc.maxSSELifetime = lifetime
	}
}

// NewSSEMCPClient creates a new SSE-based MCP client with the given base URL.
// Returns an error if the URL is invalid.
func NewSSEMCPClient(baseURL string, options ...ClientOption) (*SSEMCPClient, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	smc := &SSEMCPClient{
		baseURL:               parsedURL,
		httpClient:            &http.Client{},
		responses:             make(map[int64]chan RPCResponse),
		done:                  make(chan struct{}),
		endpointChan:          make(chan struct{}),
		sseReadTimeout:        defaultSSEReadTimeout,
		headers:               make(map[string]string),
		toolResponseSizeLimit: defaultToolResponseSizeLimit,
		responseTimeout:       defaultResponseTimeout,
	}

	for _, opt := range options {
		opt(smc)
	}

	return smc, nil
}

// Start initiates the SSE connection to the server and waits for the endpoint information.
// Returns an error if the connection fails or times out waiting for the endpoint.
func (c *SSEMCPClient) Start(ctx context.Context) error {
	// since we can't reuse SSEMCPClient, make sure we don't start it twice
	if !c.state.CompareAndSwap(int32(unInitialized), int32(started)) {
		return fmt.Errorf("mcp: client already started")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL.String(), nil)

	if err != nil {

		return fmt.Errorf("failed to create request: %w", err)

	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to SSE stream: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	go c.readSSE(ctx, resp.Body)

	// Wait for the endpoint to be received

	select {
	case <-c.endpointChan:
		// Endpoint received, proceed
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for endpoint")
	case <-time.After(30 * time.Second): // Add a timeout
		return fmt.Errorf("timeout waiting for endpoint")
	}

	return nil
}

// readSSE continuously reads the SSE stream and processes events.
// It runs until the connection is closed or an error occurs.
func (c *SSEMCPClient) readSSE(ctx context.Context, reader io.ReadCloser) {
	defer reader.Close()

	br := bufio.NewReader(reader)
	var event, data string

	ctx, cancel := context.WithTimeout(ctx, c.maxSSELifetime)
	defer cancel()

	for {
		readCtx, cancel2 := context.WithTimeout(ctx, c.sseReadTimeout)

		select {
		case <-readCtx.Done():
			// no messages received for a while, close the connection
			// or parent context is done
			cancel2() // redundant, but ensures we don't leak context
			err := c.Close()
			if err != nil {
				fmt.Printf("Error closing SSE connection: %v\n", err)
			}
			return
		default:
			line, err := br.ReadString('\n')
			cancel2()
			if err != nil {
				if err == io.EOF {
					// Process any pending event before exit
					if event != "" && data != "" {
						c.handleSSEEvent(event, data)
					}
					break
				}
				select {
				case <-c.done:
					cancel2()
					return
				default:
					fmt.Printf("SSE stream error: %v\n", err)
					cancel2()
					return
				}
			}

			// Remove only newline markers
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				// Empty line means end of event
				if event != "" && data != "" {
					c.handleSSEEvent(event, data)
					event = ""
					data = ""
				}
				cancel2()
				continue
			}

			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		cancel2()
	}
}

// handleSSEEvent processes SSE events based on their type.
// Handles 'endpoint' events for connection setup and 'message' events for JSON-RPC communication.
func (c *SSEMCPClient) handleSSEEvent(event, data string) {
	switch event {
	case "endpoint":
		endpoint, err := c.baseURL.Parse(data)
		if err != nil {
			fmt.Printf("Error parsing endpoint URL: %v\n", err)
			return
		}
		if endpoint.Host != c.baseURL.Host {
			fmt.Printf("Endpoint origin does not match connection origin\n")
			return
		}
		c.endpoint = endpoint
		close(c.endpointChan)

	case "message":
		var baseMessage struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      *int64          `json:"id,omitempty"`
			Method  string          `json:"method,omitempty"`
			Result  json.RawMessage `json:"result,omitempty"`
			Error   *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error,omitempty"`
		}

		if err := json.Unmarshal([]byte(data), &baseMessage); err != nil {
			fmt.Printf("Error unmarshaling message: %v\n", err)
			return
		}

		// Handle notification
		if baseMessage.ID == nil {
			var notification mcp.JSONRPCNotification
			if err := json.Unmarshal([]byte(data), &notification); err != nil {
				return
			}
			c.notifyMu.RLock()
			for _, handler := range c.notifications {
				handler(notification)
			}
			c.notifyMu.RUnlock()
			return
		}

		c.mu.RLock()
		ch, ok := c.responses[*baseMessage.ID]
		c.mu.RUnlock()

		if ok {
			if baseMessage.Error != nil {
				ch <- RPCResponse{
					Error: &baseMessage.Error.Message,
				}
			} else {
				ch <- RPCResponse{
					Response: &baseMessage.Result,
				}
			}
			c.mu.Lock()
			delete(c.responses, *baseMessage.ID)
			c.mu.Unlock()
		}
	}
}

// OnNotification registers a handler function to be called when notifications are received.
// Multiple handlers can be registered and will be called in the order they were added.
func (c *SSEMCPClient) OnNotification(
	handler func(notification mcp.JSONRPCNotification),
) {
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()
	c.notifications = append(c.notifications, handler)
}

// sendRequest sends a JSON-RPC request to the server and waits for a response.
// Returns the raw JSON response message or an error if the request fails.
func (c *SSEMCPClient) sendRequest(
	ctx context.Context,
	method string,
	params interface{},
) (*json.RawMessage, error) {
	currentState := connectionState(c.state.Load())
	if currentState == closed {
		return nil, fmt.Errorf("mcp: client: closed")
	}

	if !c.initialized && method != "initialize" && currentState != initialized {
		return nil, fmt.Errorf("client not initialized")
	}

	if c.endpoint == nil {
		return nil, fmt.Errorf("endpoint not received")
	}

	id := c.requestID.Add(1)

	request := mcp.JSONRPCRequest{
		JSONRPC: mcp.JSONRPC_VERSION,
		ID:      id,
		Request: mcp.Request{
			Method: method,
		},
		Params: params,
	}

	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	responseChan := make(chan RPCResponse, 1)
	c.mu.Lock()
	c.responses[id] = responseChan
	c.mu.Unlock()

	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		c.endpoint.String(),
		bytes.NewReader(requestBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	// set custom http headers
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"request failed with status %d: %s",
			resp.StatusCode,
			body,
		)
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.responses, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	case response := <-responseChan:
		if response.Error != nil {
			return nil, errors.New(*response.Error)
		}
		return response.Response, nil
	}
}

func (c *SSEMCPClient) Initialize(
	ctx context.Context,
	request mcp.InitializeRequest,
) (*mcp.InitializeResult, error) {
	// ensure we are not already initialized
	if !c.state.CompareAndSwap(int32(started), int32(initialized)) {
		return nil, fmt.Errorf("mcp: client already initialized")
	}

	// Ensure we send a params object with all required fields
	params := struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		ClientInfo      mcp.Implementation     `json:"clientInfo"`
		Capabilities    mcp.ClientCapabilities `json:"capabilities"`
	}{
		ProtocolVersion: request.Params.ProtocolVersion,
		ClientInfo:      request.Params.ClientInfo,
		Capabilities:    request.Params.Capabilities, // Will be empty struct if not set
	}

	response, err := c.sendRequest(ctx, "initialize", params)
	if err != nil {
		return nil, err
	}

	var result mcp.InitializeResult
	if err := json.Unmarshal(*response, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Store capabilities
	c.capabilities = result.Capabilities

	// Send initialized notification
	notification := mcp.JSONRPCNotification{
		JSONRPC: mcp.JSONRPC_VERSION,
		Notification: mcp.Notification{
			Method: "notifications/initialized",
		},
	}

	notificationBytes, err := json.Marshal(notification)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to marshal initialized notification: %w",
			err,
		)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		c.endpoint.String(),
		bytes.NewReader(notificationBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create notification request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to send initialized notification: %w",
			err,
		)
	}
	resp.Body.Close()

	c.initialized = true
	return &result, nil
}

func (c *SSEMCPClient) Ping(ctx context.Context) error {
	_, err := c.sendRequest(ctx, "ping", nil)
	return err
}

func (c *SSEMCPClient) ListResources(
	ctx context.Context,
	request mcp.ListResourcesRequest,
) (*mcp.ListResourcesResult, error) {
	response, err := c.sendRequest(ctx, "resources/list", request.Params)
	if err != nil {
		return nil, err
	}

	var result mcp.ListResourcesResult
	if err := json.Unmarshal(*response, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

func (c *SSEMCPClient) ListResourceTemplates(
	ctx context.Context,
	request mcp.ListResourceTemplatesRequest,
) (*mcp.ListResourceTemplatesResult, error) {
	response, err := c.sendRequest(
		ctx,
		"resources/templates/list",
		request.Params,
	)
	if err != nil {
		return nil, err
	}

	var result mcp.ListResourceTemplatesResult
	if err := json.Unmarshal(*response, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

func (c *SSEMCPClient) ReadResource(
	ctx context.Context,
	request mcp.ReadResourceRequest,
) (*mcp.ReadResourceResult, error) {
	response, err := c.sendRequest(ctx, "resources/read", request.Params)
	if err != nil {
		return nil, err
	}

	return mcp.ParseReadResourceResult(response)
}

func (c *SSEMCPClient) Subscribe(
	ctx context.Context,
	request mcp.SubscribeRequest,
) error {
	_, err := c.sendRequest(ctx, "resources/subscribe", request.Params)
	return err
}

func (c *SSEMCPClient) Unsubscribe(
	ctx context.Context,
	request mcp.UnsubscribeRequest,
) error {
	_, err := c.sendRequest(ctx, "resources/unsubscribe", request.Params)
	return err
}

func (c *SSEMCPClient) ListPrompts(
	ctx context.Context,
	request mcp.ListPromptsRequest,
) (*mcp.ListPromptsResult, error) {
	response, err := c.sendRequest(ctx, "prompts/list", request.Params)
	if err != nil {
		return nil, err
	}

	var result mcp.ListPromptsResult
	if err := json.Unmarshal(*response, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

func (c *SSEMCPClient) GetPrompt(
	ctx context.Context,
	request mcp.GetPromptRequest,
) (*mcp.GetPromptResult, error) {
	response, err := c.sendRequest(ctx, "prompts/get", request.Params)
	if err != nil {
		return nil, err
	}

	return mcp.ParseGetPromptResult(response)
}

func (c *SSEMCPClient) ListTools(
	ctx context.Context,
	request mcp.ListToolsRequest,
) (*mcp.ListToolsResult, error) {
	response, err := c.sendRequest(ctx, "tools/list", request.Params)
	if err != nil {
		return nil, err
	}

	var result mcp.ListToolsResult
	if err := json.Unmarshal(*response, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

func (c *SSEMCPClient) CallTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.responseTimeout)
	defer cancel()

	response, err := c.sendRequest(ctx, "tools/call", request.Params)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: %s", ErrResponseTimeout, err.Error())
		}
		return nil, err
	}

	if response != nil && len(*response) > c.toolResponseSizeLimit {
		return nil, fmt.Errorf("%w: response size (%d bytes) exceeds the configured limit (%d bytes)",
			ErrResponseTooLarge,
			len(*response), c.toolResponseSizeLimit)
	}

	return mcp.ParseCallToolResult(response)
}

func (c *SSEMCPClient) SetLevel(
	ctx context.Context,
	request mcp.SetLevelRequest,
) error {
	_, err := c.sendRequest(ctx, "logging/setLevel", request.Params)
	return err
}

func (c *SSEMCPClient) Complete(
	ctx context.Context,
	request mcp.CompleteRequest,
) (*mcp.CompleteResult, error) {
	response, err := c.sendRequest(ctx, "completion/complete", request.Params)
	if err != nil {
		return nil, err
	}

	var result mcp.CompleteResult
	if err := json.Unmarshal(*response, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

// Helper methods

// GetEndpoint returns the current endpoint URL for the SSE connection.
func (c *SSEMCPClient) GetEndpoint() *url.URL {
	return c.endpoint
}

// Close shuts down the SSE client connection and cleans up any pending responses.
// Returns an error if the shutdown process fails.
func (c *SSEMCPClient) Close() error {
	if !c.state.CompareAndSwap(int32(started), int32(closed)) &&
		!c.state.CompareAndSwap(int32(initialized), int32(closed)) {
		return nil
	}
	
	select {
	case <-c.done:
		return nil // Already closed
	default:
		close(c.done)
	}

	// Clean up any pending responses
	c.mu.Lock()
	for _, ch := range c.responses {
		close(ch)
	}
	c.responses = make(map[int64]chan RPCResponse)
	c.mu.Unlock()

	return nil
}
