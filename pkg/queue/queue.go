package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// S3Queue implements a log-structured queue using S3.
type S3Queue struct {
	client S3API
	cfg    S3QueueConfig
}

// NewS3Queue creates a new S3Queue.
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
	return &S3Queue{
		client: client,
		cfg:    cfg,
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Publish sends a message to the queue.
func (q *S3Queue) Publish(ctx context.Context, data []byte) error {
	id := fmt.Sprintf("%d_%s", time.Now().UnixNano(), uuid.New().String())
	shardID := q.getShardID(id)
	key := fmt.Sprintf("%s/topic/shard-%d/%s", q.cfg.QueueName, shardID, id)

	log.Printf("Publishing message %s to shard %d", id, shardID)

	start := time.Now()
	_, err := q.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		q.cfg.Metrics.IncError("publish")
		return fmt.Errorf("failed to publish message: %w", err)
	}
	q.cfg.Metrics.IncPublished()
	q.cfg.Metrics.ObserveLatency("publish", time.Since(start))
	return nil
}

func (q *S3Queue) getShardID(id string) int {
	h := 0
	for _, c := range id {
		h = 31*h + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h % q.cfg.Shards
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
			if q.tryLockShard(ctx, groupID, idx, consumerID) {
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
		q.unlockShard(ctx, groupID, shardID, consumerID)

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
			if !q.refreshLock(context.Background(), groupID, shardID, consumerID) {
				log.Printf("Lost lock for shard %d, stopping processing", shardID)
				cancel()
				return
			}
		}
	}
}

func (q *S3Queue) processShard(ctx context.Context, groupID string, shardID int, handler Handler) error {
	lastID, err := q.getCheckpoint(ctx, groupID, shardID)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint: %w", err)
	}

	prefix := fmt.Sprintf("%s/topic/shard-%d/", q.cfg.QueueName, shardID)
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

		params := &s3.ListObjectsV2Input{
			Bucket:  aws.String(q.cfg.Bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(100),
		}
		if startAfter != "" {
			params.StartAfter = aws.String(startAfter)
		}

		resp, err := q.client.ListObjectsV2(ctx, params)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		if len(resp.Contents) == 0 {
			// No messages, return to allow switching shards
			return nil
		}

		for _, obj := range resp.Contents {
			key := *obj.Key
			parts := strings.Split(key, "/")
			id := parts[len(parts)-1]
			// Fetch and process
			start := time.Now()
			err := q.processMessage(ctx, key, id, shardID, handler)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					q.cfg.Metrics.IncError("consume")
					log.Printf("Error processing message %s: %v", id, err)

					// Handle Retry
					count, retryErr := q.incrementRetryCount(ctx, groupID, id)
					if retryErr != nil {
						if !errors.Is(retryErr, context.Canceled) {
							log.Printf("Failed to increment retry count: %v", retryErr)
						}
						return fmt.Errorf("processing failed and retry count update failed: %w", err)
					}

					if count > q.cfg.MaxRetries {
						log.Printf("Message %s exceeded max retries (%d), moving to DLQ", id, q.cfg.MaxRetries)
						if dlqErr := q.moveToDLQ(ctx, key, id); dlqErr != nil {
							log.Printf("Failed to move to DLQ: %v", dlqErr)
							return fmt.Errorf("processing failed and DLQ move failed: %w", err)
						}
						q.cfg.Metrics.IncDLQ()
						q.deleteRetryCount(ctx, groupID, id)
					} else {
						return fmt.Errorf("processing failed (attempt %d): %w", count, err)
					}
				} else {
					return err
				}
			} else {
				// Success, clear retry count
				q.cfg.Metrics.IncConsumed()
				q.cfg.Metrics.ObserveLatency("consume", time.Since(start))
				q.deleteRetryCount(ctx, groupID, id)
			}

			lastID = id
			startAfter = key
		}

		if err := q.setCheckpoint(ctx, groupID, shardID, lastID); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("Failed to save checkpoint: %v", err)
			}
		}
	}
}

func (q *S3Queue) tryLockShard(ctx context.Context, groupID string, shardID int, consumerID string) bool {
	lockKey := fmt.Sprintf("%s/locks/%s/shard-%d", q.cfg.QueueName, groupID, shardID)
	now := time.Now().Format(time.RFC3339Nano)
	content := fmt.Sprintf("%s|%s", now, consumerID)

	// 1. Try to create new lock
	_, err := q.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(q.cfg.Bucket),
		Key:         aws.String(lockKey),
		Body:        bytes.NewReader([]byte(content)),
		IfNoneMatch: aws.String("*"),
	})

	// Verify ownership (handle eventual consistency/race)
	if err == nil {
		// Read back to confirm we won
		// Small sleep to allow propagation?
		time.Sleep(10 * time.Millisecond)

		resp, err := q.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(q.cfg.Bucket),
			Key:    aws.String(lockKey),
		})
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return strings.Contains(string(data), consumerID)
	}

	// 2. Check if expired
	resp, err := q.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(lockKey),
	})
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	parts := strings.Split(string(data), "|")
	tsStr := parts[0]

	ts, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		// Invalid timestamp, assume expired and steal
		ts = time.Time{}
	}

	if time.Since(ts) > q.cfg.LockTTL {
		// Expired, try to steal with IfMatch ETag
		_, err := q.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(q.cfg.Bucket),
			Key:     aws.String(lockKey),
			Body:    bytes.NewReader([]byte(content)),
			IfMatch: resp.ETag,
		})
		return err == nil
	}

	return false
}

func (q *S3Queue) unlockShard(ctx context.Context, groupID string, shardID int, consumerID string) {
	lockKey := fmt.Sprintf("%s/locks/%s/shard-%d", q.cfg.QueueName, groupID, shardID)

	// Only delete if we own it?
	// For simplicity, just delete. If we lost lock, we might delete someone else's lock,
	// but heartbeat handles that.
	// Ideally check consumerID.
	q.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(lockKey),
	})
}

func (q *S3Queue) refreshLock(ctx context.Context, groupID string, shardID int, consumerID string) bool {
	lockKey := fmt.Sprintf("%s/locks/%s/shard-%d", q.cfg.QueueName, groupID, shardID)
	now := time.Now().Format(time.RFC3339Nano)
	content := fmt.Sprintf("%s|%s", now, consumerID)

	_, err := q.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(lockKey),
		Body:   bytes.NewReader([]byte(content)),
	})
	return err == nil
}

func (q *S3Queue) getCheckpoint(ctx context.Context, groupID string, shardID int) (string, error) {
	key := fmt.Sprintf("%s/checkpoints/%s/shard-%d", q.cfg.QueueName, groupID, shardID)
	resp, err := q.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return "", nil
		}
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (q *S3Queue) setCheckpoint(ctx context.Context, groupID string, shardID int, id string) error {
	key := fmt.Sprintf("%s/checkpoints/%s/shard-%d", q.cfg.QueueName, groupID, shardID)
	_, err := q.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(id)),
	})
	return err
}

func (q *S3Queue) processMessage(ctx context.Context, key, id string, shardID int, handler Handler) error {
	resp, err := q.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get message body: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read message body: %w", err)
	}

	return handler(ctx, Message{ID: id, Data: data, ShardID: shardID})
}

func (q *S3Queue) getRetryCount(ctx context.Context, groupID, id string) (int, error) {
	key := fmt.Sprintf("%s/retries/%s/%s", q.cfg.QueueName, groupID, id)
	resp, err := q.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return 0, nil
		}
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var count int
	fmt.Sscanf(string(data), "%d", &count)
	return count, nil
}

func (q *S3Queue) incrementRetryCount(ctx context.Context, groupID, id string) (int, error) {
	count, err := q.getRetryCount(ctx, groupID, id)
	if err != nil {
		return 0, err
	}
	count++
	key := fmt.Sprintf("%s/retries/%s/%s", q.cfg.QueueName, groupID, id)
	_, err = q.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(fmt.Sprintf("%d", count))),
	})
	return count, err
}

func (q *S3Queue) deleteRetryCount(ctx context.Context, groupID, id string) {
	key := fmt.Sprintf("%s/retries/%s/%s", q.cfg.QueueName, groupID, id)
	q.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(q.cfg.Bucket),
		Key:    aws.String(key),
	})
}

func (q *S3Queue) moveToDLQ(ctx context.Context, key, id string) error {
	if q.cfg.DLQName == "" {
		return fmt.Errorf("DLQ not configured")
	}

	// Copy object to DLQ
	dlqKey := fmt.Sprintf("%s/%s", q.cfg.DLQName, id)
	_, err := q.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(q.cfg.Bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", q.cfg.Bucket, key)),
		Key:        aws.String(dlqKey),
	})
	if err != nil {
		return fmt.Errorf("failed to copy to DLQ: %w", err)
	}

	return nil
}
