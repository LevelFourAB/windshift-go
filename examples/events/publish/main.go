package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/levelfourab/windshift-go"
	"github.com/levelfourab/windshift-go/events/streams"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	parallelism := flag.Int("parallelism", 1, "Number of parallel publishers")
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

	_, err = eventsClient.EnsureStream(context.Background(), "test", streams.WithSubjects("test"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	total := atomic.Int32{}
	processed := atomic.Int32{}
	go func() {
		// Print the counter every second
		timer := time.NewTicker(time.Second)
		for {
			<-timer.C
			current := processed.Swap(0)
			t := total.Add(current)
			log.Println("Generated", current, "events, total=", t)
		}
	}()

	current := atomic.Int32{}

	// Start a goroutine for each parallel publisher
	for j := 0; j < *parallelism; j++ {
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}

				v := current.Add(1)
				value := strconv.Itoa(int(v))

				_, err := eventsClient.Publish(ctx, "test", structpb.NewStringValue(value))
				if err != nil {
					log.Println("Could not send event", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// log.Println("Sent event: ", v)
				processed.Add(1)
			}
		}()
	}

	<-ctx.Done()
}
