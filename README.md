# windshift-go

A Go-library to interact with a [Windshift server](https://github.com/levelfourab/windshift-server)
to manage and consume events.

## Using

```bash
go get github.com/levelfourab/windshift-go
```

Creating a client:

```go
client, err := windshift.NewClient("localhost:8080", windshift.WithGRPCOptions(
  grpc.WithTransportCredentials(insecure.NewCredentials()),
))
```

## Protobuf

Data in Windshift is represented using [Protobuf](https://protobuf.dev/). You
will need to generate Go code from your Protobuf files to use the library.

## Events

The events package provides a way to publish and consume events. A client
for handling events can be fetched using the `Events` method on the main
client:

```go
eventsClient := client.Events()
```

### Defining streams

Streams in Windshift store events. They can be used to replay events, or to
store events for a certain amount of time. Streams can be configured with
retention policies, sources, and more.

See [Streams in the NATS Jetstream documentation](https://docs.nats.io/nats-concepts/jetstream/streams)
for more details.

Set up a stream that stores events published to subjects starting with `orders.`:

```go
stream, err := eventsClient.EnsureStream(
  ctx,
  "orders", 
  streams.WithSubjects("orders.>"),
)
```

### Publishing an event

Events can be published if there is a stream that matches the subject of the
event. The event will be stored in the stream and can be consumed by consumers
subscribed to the stream.

Example:

```go
eventsClient.Publish(ctx, &events.OutgoingEvent{
  Subject: "orders.created",
  Data: &ordersv1.OrderCreated{
    ID: "123",
  },
})
```

### Defining a consumer

Consumers in Windshift are used to subscribe to events. Consumers can can be
ephemeral or durable. Ephemeral consumers are automatically removed after
they have been inactive for a certain amount of time.

To create a durable consumer give it a name:

```go
consumer, err := events.EnsureConsumer(ctx, "orders", consumers.WithName("idOfConsumer"))
```

To create an ephemeral consumer omit the name:

```go
consumer, err := events.EnsureConsumer(ctx, "orders")
```

Consumers can be configured with options. Options include:

* `WithSubjects` - a subset of subjects to subscribe to.
* `WithProcessingTimeout` - the time to wait for an event to be acknowledged,
  rejected, or pinged before requeuing it.
* `WithConsumeFrom` - the position in the stream to start consuming events from.

### Subscribing to events

```go
events, err := eventsClient.Subscribe(ctx, "orders", "idOfConsumer")

for event := range events {
  // Process event
  data, err := event.UnmarshalNew()
  
  // Acknowledge that event was processed
  err := event.Ack()
  if err != nil {
    // Handle error
  }
}
```

The library does not handle reconnection and retries, so often you may want
to subscribe to events in a loop:

```go
for {
  events, err := eventsClient.Subscribe(ctx, "orders", "idOfConsumer")
  if err != nil {
    // Optionally sleep according to a backoff strategy
    time.Sleep(1 * time.Second)
    
    if ctx.Err() != nil {
      // The context is done, we should not try to reconnect
      break
    }

    continue
  }

  for event := range events {
    // Process event directly or pass event to a worker pool. Make sure to
    // Acknowledge or Reject the event. For long running tasks, make sure to
    // Ping the event so it does not get requeued.
  }

  // At this point the subscription was closed, either due to the context being
  // canceled, or the connection to the server was lost.
  if ctx.Err() != nil {
    // Late check to avoid a sleep after the context is done
    break
  }

  // Sleep for a bit before trying to reconnect
  time.Sleep(100 * time.Millisecond)
}
```
