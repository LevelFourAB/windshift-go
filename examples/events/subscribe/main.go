package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/levelfourab/windshift-go"
	"github.com/levelfourab/windshift-go/events"
	"github.com/levelfourab/windshift-go/events/consumers"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/levelfourab/windshift-go/events/subscribe"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	// Get a flag to determine if individual events should be printed
	printEvents := flag.Bool("print-events", false, "Print individual events")
	workLoadTime := flag.Int("work-load-time", 100, "Time to fake processing an event in milliseconds")
	parallelism := flag.Uint("parallelism", 1, "Number of parallel consumers")
	flag.Parse()

	natsClient, err := nats.Connect("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer natsClient.Close()

	eventsClient, err := windshift.NewEvents(natsClient)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	// Create a consumer on the existing stream `test`
	consumer, err := eventsClient.EnsureConsumer(
		ctx,
		"test",
		consumers.WithConsumeFrom(streams.AtStreamStart()),
	)
	if err != nil {
		log.Fatal(err)
	}

	eventChannel, err := eventsClient.Subscribe(ctx, "test", consumer.Name(), subscribe.MaxProcessingEvents(*parallelism))
	if err != nil {
		log.Fatal(err)
	}

	pending := atomic.Int32{}
	processing := atomic.Int32{}
	processed := atomic.Int32{}
	go func() {
		// Print the counter every second
		timer := time.NewTicker(time.Second)
		for {
			<-timer.C
			current := processed.Swap(0)
			log.Println("Processed", current, "events, in progress=", processing.Load(), "pending=", pending.Load())
		}
	}()

	workQueue := make(chan events.Event, *parallelism*5)
	for i := uint(0); i < *parallelism; i++ {
		go func() {
			for {
				event := <-workQueue
				pending.Add(-1)

				processing.Add(1)
				data, err := event.UnmarshalNew()
				if err != nil {
					log.Println("Could not unmarshal event data:", err)
					return
				}

				switch d := data.(type) {
				case *structpb.Value:
					if *printEvents {
						log.Println("Received event", "id=", event.ID(), "time=", event.Headers().OccurredAt(), "value=", d.GetStringValue())
					}
				}

				// Fake some processing time
				sleepInMS := *workLoadTime
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)

				err = event.Ack(ctx)
				if err != nil {
					log.Fatal(err)
				}
				processed.Add(1)
				processing.Add(-1)
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-eventChannel:
			// Acquire a semaphore slot
			pending.Add(1)
			workQueue <- e
		}
	}
}
