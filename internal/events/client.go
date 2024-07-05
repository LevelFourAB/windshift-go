package events

import (
	"github.com/levelfourab/windshift-go/events"
	eventsv1alpha1 "github.com/levelfourab/windshift-go/internal/proto/windshift/events/v1alpha1"
	"google.golang.org/grpc"
)

type Client struct {
	service eventsv1alpha1.EventsServiceClient
}

func New(conn *grpc.ClientConn) *Client {
	return &Client{
		service: eventsv1alpha1.NewEventsServiceClient(conn),
	}
}

var _ events.Client = (*Client)(nil)
