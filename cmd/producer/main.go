package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"s3-queue/pkg/queue"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name")
	queueName := flag.String("queue", "default-queue", "Queue name")
	msgCount := flag.Int("count", 1, "Number of messages to send")
	flag.Parse()

	if *bucket == "" {
		log.Fatal("Bucket name is required")
	}

	ctx := context.TODO()
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

	for i := 0; i < *msgCount; i++ {
		msg := fmt.Sprintf("Message %d at %s", i, time.Now().Format(time.RFC3339))
		err := q.Publish(ctx, []byte(msg))
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
		} else {
			fmt.Printf("Published: %s\n", msg)
		}
	}
}
