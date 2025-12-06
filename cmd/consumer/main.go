package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"s3-queue/pkg/queue"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name")
	queueName := flag.String("queue", "default-queue", "Queue name")
	flag.Parse()

	if *bucket == "" {
		log.Fatal("Bucket name is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := s3.NewFromConfig(cfg)
	q := queue.NewS3Queue(client, queue.S3QueueConfig{
		Bucket:    *bucket,
		QueueName: *queueName,
		Shards:    10,
	})

	log.Printf("Starting consumer for queue: %s", *queueName)
	err = q.Consume(ctx, "default-group", func(ctx context.Context, msg queue.Message) error {
		fmt.Printf("Received: %s (ID: %s)\n", string(msg.Data), msg.ID)
		// Simulate work
		// time.Sleep(100 * time.Millisecond)
		return nil
	})

	if err != nil && err != context.Canceled {
		log.Fatalf("Consumer error: %v", err)
	}
}
