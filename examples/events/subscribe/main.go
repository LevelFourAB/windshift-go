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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	testv1 "github.com/levelfourab/windshift-go/internal/proto/windshift/test/v1"
)

func main() {
	// Get a flag to determine if individual events should be printed
	printEvents := flag.Bool("print-events", false, "Print individual events")
	workLoadTime := flag.Int("work-load-time", 100, "Time to fake processing an event in milliseconds")
	parallelism := flag.Int("parallelism", 1, "Number of parallel consumers")
	flag.Parse()

	client, err := windshift.NewClient("localhost:8080", windshift.WithGRPCOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	eventsClient := client.Events()

	// Create a consumer on the existing stream `test`
	consumer, err := eventsClient.EnsureConsumer(
		ctx,
		"test",
		consumers.WithSubjects("test"),
		consumers.WithConsumeFrom(streams.AtStreamStart()),
	)
	if err != nil {
		log.Fatal(err)
	}

	eventChannel, err := eventsClient.Subscribe(ctx, "test", consumer.ID(), subscribe.MaxProcessingEvents(*parallelism))
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

	workQueue := make(chan events.Event, 1000)
	for i := 0; i < *parallelism; i++ {
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
				case *testv1.StringValue:
					if *printEvents {
						log.Println("Received event", "id=", event.ID(), "time=", event.Headers().OccurredAt(), "value=", d.Value)
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
