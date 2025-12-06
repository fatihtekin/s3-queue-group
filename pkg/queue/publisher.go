package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

// Publisher handles message publishing to the queue.
type Publisher interface {
	Publish(ctx context.Context, data []byte) error
}

// S3Publisher implements Publisher using S3 message store.
type S3Publisher struct {
	messages  MessageStore
	metrics   MetricsReporter
	shards    int
	queueName string
}

// NewS3Publisher creates a new S3-based publisher.
func NewS3Publisher(messages MessageStore, metrics MetricsReporter, shards int, queueName string) *S3Publisher {
	return &S3Publisher{
		messages:  messages,
		metrics:   metrics,
		shards:    shards,
		queueName: queueName,
	}
}

// Publish sends a message to the queue.
func (p *S3Publisher) Publish(ctx context.Context, data []byte) error {
	id := fmt.Sprintf("%d_%s", time.Now().UnixNano(), uuid.New().String())
	shardID := p.getShardID(id)

	log.Printf("Publishing message %s to shard %d", id, shardID)

	start := time.Now()
	err := p.messages.Publish(ctx, shardID, id, data)
	if err != nil {
		p.metrics.IncError("publish")
		return fmt.Errorf("failed to publish message: %w", err)
	}
	p.metrics.IncPublished()
	p.metrics.ObserveLatency("publish", time.Since(start))
	return nil
}

// getShardID computes the shard ID for a message using a simple hash.
func (p *S3Publisher) getShardID(id string) int {
	h := 0
	for _, c := range id {
		h = 31*h + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h % p.shards
}
