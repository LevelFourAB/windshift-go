# windshift-go

Windshift is an event-stream framework on top of [NATS Jetstream](https://nats.io/),
that enforces strong typing using [Protobuf](https://protobuf.dev/) with support
for tracing using [OpenTelemetry](https://opentelemetry.io).

## Features

- 🌊 Stream management, declare streams and bind subjects to them, with
  configurable retention and limits
- 📄 Event data in Protobuf format, for strong typing and schema evolution
- 📤 Publish events to subjects, with idempotency and [OpenTelemetry tracing](https://opentelemetry.io)
- 📥 Durable consumers with distributed processing
- 🕒 Ephemeral consumers for one off event processing
- 🔄 Automatic redelivery of failed events
- 🔔 Ability to extend processing time by pinging events

## Using

```bash
go get github.com/levelfourab/windshift-go
```

Creating a client:

```go
natsClient, err := nats.Connect("localhost:4222")
eventsClient, err := windshift.NewEvents(natsClient)
```

## Protobuf

Data in Windshift is represented using [Protobuf](https://protobuf.dev/). You
will need to generate Go code from your Protobuf files to use the library.

[Buf](https://buf.build/) can be used to generate the necessary files, and
provides linting and breaking change detection.

## Streams

Streams in Windshift store events. They can be used to replay events, or to
store events for a certain amount of time. Streams can be configured with
retention policies, sources, and more.

Streams can receive events from multiple subjects, but each subject can only
be bound to one stream. For example, if you have a stream called `orders` which
receives events from the `orders.created` subject, you cannot create another
stream that also receives events from `orders.created`.

See [Streams in the NATS Jetstream documentation](https://docs.nats.io/nats-concepts/jetstream/streams)
for more details.

Example:

```go
stream, err := eventsClient.EnsureStream(
  ctx,
  "orders", 
  streams.WithSubjects("orders.>"),
)
```

It is possible to control the retention policy of the stream via things like
`streams.MaxAge`, `streams.MaxBytes`, and `streams.MaxEvents`.

Example:

```go
stream, err := eventsClient.EnsureStream(
  ctx,
  "orders", 
  streams.WithSubjects("orders.>"),
  streams.MaxAge(30 * time.Days),
)
```

## Publishing events

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

Features:

- Timestamps for when the event occurred can be specified with `timestamp`.
- Idempotency keys can be specified using `idempotency_key`. If an event with
  the same idempotency key has already been published, the event will not be
  published again. The window for detecting duplicates can be configured via
  the stream.
- Optimistic concurrency control can be used via `expected_last_id`. If the
  last event in the stream does not have the specified id, the event will not
  be published.

## Defining a consumer

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

- `WithSubjects` - a subset of subjects to subscribe to.
- `WithProcessingTimeout` - the time to wait for an event to be acknowledged,
  rejected, or pinged before requeuing it.
- `WithConsumeFrom` - the position in the stream to start consuming events from.

## Subscribing to events

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
