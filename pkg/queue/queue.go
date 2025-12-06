package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// Message represents a message in the queue.
type Message struct {
	ID      string
	Data    []byte
	ShardID int
}

// S3API defines the methods we use from the S3 client.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
}

// S3QueueConfig holds configuration for the queue.
type S3QueueConfig struct {
	Bucket       string
	QueueName    string
	Shards       int
	PollInterval time.Duration
	LockTTL      time.Duration
	MaxRetries   int
	DLQName      string
	Metrics      MetricsReporter
}

// S3Queue implements a log-structured queue using S3 with pluggable components.
type S3Queue struct {
	publisher    Publisher
	locker       Locker
	checkpoint   CheckpointStore
	retryManager RetryManager
	messages     MessageStore
	metrics      MetricsReporter
	cfg          S3QueueConfig
}

// NewS3Queue creates a new S3Queue with injected dependencies.
func NewS3Queue(client S3API, cfg S3QueueConfig) *S3Queue {
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 30 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &NoopMetricsReporter{}
	}

	messages := NewS3MessageStore(client, cfg.Bucket, cfg.QueueName)

	return &S3Queue{
		publisher:    NewS3Publisher(messages, cfg.Metrics, cfg.Shards, cfg.QueueName),
		locker:       NewS3Locker(client, cfg.Bucket, cfg.QueueName, cfg.LockTTL),
		checkpoint:   NewS3CheckpointStore(client, cfg.Bucket, cfg.QueueName),
		retryManager: NewS3RetryManager(client, cfg.Bucket, cfg.QueueName, cfg.MaxRetries),
		messages:     messages,
		metrics:      cfg.Metrics,
		cfg:          cfg,
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Publish sends a message to the queue.
func (q *S3Queue) Publish(ctx context.Context, data []byte) error {
	return q.publisher.Publish(ctx, data)
}

// Handler is a function that processes a message.
type Handler func(ctx context.Context, msg Message) error

// Consume starts a consumer loop for a specific consumer group.
func (q *S3Queue) Consume(ctx context.Context, groupID string, handler Handler) error {
	consumerID := uuid.New().String()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		shardID := -1
		startIdx := rand.Intn(q.cfg.Shards)
		for i := 0; i < q.cfg.Shards; i++ {
			idx := (startIdx + i) % q.cfg.Shards
			acquired, err := q.locker.TryAcquire(ctx, groupID, idx, consumerID)
			if err == nil && acquired {
				shardID = idx
				break
			}
		}

		if shardID == -1 {
			time.Sleep(q.cfg.PollInterval)
			continue
		}

		log.Printf("Acquired lock for shard %d (consumer %s)", shardID, consumerID)

		// Create a context that is canceled if we lose the lock
		shardCtx, cancel := context.WithCancel(ctx)

		// Start heartbeat
		go q.heartbeat(shardCtx, cancel, groupID, shardID, consumerID)

		err := q.processShard(shardCtx, groupID, shardID, handler)

		cancel() // Stop heartbeat
		q.locker.Release(ctx, groupID, shardID, consumerID)

		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("Error processing shard %d: %v", shardID, err)
			}
			time.Sleep(q.cfg.PollInterval)
		} else {
			// Add a small sleep to prevent tight loops when shards are empty (work stealing)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (q *S3Queue) heartbeat(ctx context.Context, cancel context.CancelFunc, groupID string, shardID int, consumerID string) {
	ticker := time.NewTicker(q.cfg.LockTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := q.locker.Refresh(context.Background(), groupID, shardID, consumerID); err != nil {
				log.Printf("Lost lock for shard %d, stopping processing", shardID)
				cancel()
				return
			}
		}
	}
}

func (q *S3Queue) processShard(ctx context.Context, groupID string, shardID int, handler Handler) error {
	lastID, err := q.checkpoint.Get(ctx, groupID, shardID)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint: %w", err)
	}

	startAfter := ""
	if lastID != "" {
		startAfter = fmt.Sprintf("%s/topic/shard-%d/%s", q.cfg.QueueName, shardID, lastID)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// List messages in the shard
		refs, err := q.messages.List(ctx, shardID, startAfter, 100)
		if err != nil {
			return fmt.Errorf("failed to list messages: %w", err)
		}

		if len(refs) == 0 {
			// No messages, return to allow switching shards
			return nil
		}

		for _, ref := range refs {
			// Fetch and process
			start := time.Now()
			data, err := q.messages.Fetch(ctx, ref.Key)
			if err != nil {
				return fmt.Errorf("failed to fetch message: %w", err)
			}

			err = handler(ctx, Message{ID: ref.MessageID, Data: data, ShardID: shardID})
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					q.metrics.IncError("consume")
					log.Printf("Error processing message %s: %v", ref.MessageID, err)

					// Handle Retry
					count, retryErr := q.retryManager.Increment(ctx, groupID, ref.MessageID)
					if retryErr != nil {
						if !errors.Is(retryErr, context.Canceled) {
							log.Printf("Failed to increment retry count: %v", retryErr)
						}
						return fmt.Errorf("processing failed and retry count update failed: %w", err)
					}

					if q.retryManager.ShouldMoveToDLQ(count) {
						log.Printf("Message %s exceeded max retries (%d), moving to DLQ", ref.MessageID, q.cfg.MaxRetries)
						if dlqErr := q.messages.MoveToDLQ(ctx, ref.Key, ref.MessageID, q.cfg.DLQName); dlqErr != nil {
							log.Printf("Failed to move to DLQ: %v", dlqErr)
							return fmt.Errorf("processing failed and DLQ move failed: %w", err)
						}
						q.metrics.IncDLQ()
						q.retryManager.Delete(ctx, groupID, ref.MessageID)
					} else {
						return fmt.Errorf("processing failed (attempt %d): %w", count, err)
					}
				} else {
					return err
				}
			} else {
				// Success, clear retry count
				q.metrics.IncConsumed()
				q.metrics.ObserveLatency("consume", time.Since(start))
				q.retryManager.Delete(ctx, groupID, ref.MessageID)
			}

			lastID = ref.MessageID
			startAfter = ref.Key
		}

		if err := q.checkpoint.Set(ctx, groupID, shardID, lastID); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("Failed to save checkpoint: %v", err)
			}
		}
	}
}
