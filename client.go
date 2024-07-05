package windshift

import (
	"github.com/levelfourab/windshift-go/events"
	eventsimpl "github.com/levelfourab/windshift-go/internal/events"
	"google.golang.org/grpc"
)

type Client interface {
	// Events return the client to use for managing and subscribing to events.
	Events() events.Client

	// Close the client.
	Close() error
}

type grpcClient struct {
	conn *grpc.ClientConn

	events events.Client
}

// NewClient creates a new client to a Windshift server.
func NewClient(url string, opts ...ClientOption) (Client, error) {
	options := &clientOptions{}
	for _, o := range opts {
		o(options)
	}

	conn, err := grpc.NewClient(url, options.grpcOptions...)
	if err != nil {
		return nil, err
	}

	return NewClientWithConn(conn)
}

// NewClientWithConn creates a new client using an existing gRPC connection.
func NewClientWithConn(conn *grpc.ClientConn) (Client, error) {
	c := &grpcClient{
		conn: conn,
	}

	c.events = eventsimpl.New(conn)
	return c, nil
}

func (c *grpcClient) Events() events.Client {
	return c.events
}

func (c *grpcClient) Close() error {
	return c.conn.Close()
}

type clientOptions struct {
	grpcOptions []grpc.DialOption
}

// ClientOption is an option to configure the client.
type ClientOption func(*clientOptions)

// WithGRPCOptions sets the options to use for gRPC connections.
func WithGRPCOptions(opts ...grpc.DialOption) ClientOption {
	return func(o *clientOptions) {
		o.grpcOptions = opts
	}
}
