package queue

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Locker manages distributed shard locks using S3.
type Locker interface {
	TryAcquire(ctx context.Context, groupID string, shardID int, consumerID string) (bool, error)
	Refresh(ctx context.Context, groupID string, shardID int, consumerID string) error
	Release(ctx context.Context, groupID string, shardID int, consumerID string) error
}

// S3Locker implements Locker using S3 objects with conditional writes.
type S3Locker struct {
	client    S3API
	bucket    string
	queueName string
	lockTTL   time.Duration
}

// NewS3Locker creates a new S3-based locker.
func NewS3Locker(client S3API, bucket, queueName string, lockTTL time.Duration) *S3Locker {
	return &S3Locker{
		client:    client,
		bucket:    bucket,
		queueName: queueName,
		lockTTL:   lockTTL,
	}
}

// TryAcquire attempts to acquire a lock on a shard for a consumer.
// Returns true if the lock was acquired, false otherwise.
func (l *S3Locker) TryAcquire(ctx context.Context, groupID string, shardID int, consumerID string) (bool, error) {
	lockKey := l.lockKey(groupID, shardID)
	now := time.Now().Format(time.RFC3339Nano)
	content := fmt.Sprintf("%s|%s", now, consumerID)

	// 1. Try to create new lock using IfNoneMatch for atomic creation
	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(l.bucket),
		Key:         aws.String(lockKey),
		Body:        bytes.NewReader([]byte(content)),
		IfNoneMatch: aws.String("*"),
	})

	// If creation succeeded, verify ownership (handle eventual consistency)
	if err == nil {
		// Small sleep to allow S3 propagation
		time.Sleep(10 * time.Millisecond)

		resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(l.bucket),
			Key:    aws.String(lockKey),
		})
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return strings.Contains(string(data), consumerID), nil
	}

	// 2. Lock exists, check if expired and try to steal
	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(lockKey),
	})
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, nil
	}

	parts := strings.Split(string(data), "|")
	if len(parts) < 2 {
		// Invalid format, try to steal
		return l.stealLock(ctx, lockKey, content, resp.ETag)
	}

	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		// Invalid timestamp, try to steal
		return l.stealLock(ctx, lockKey, content, resp.ETag)
	}

	// Check if lock is expired
	if time.Since(ts) > l.lockTTL {
		return l.stealLock(ctx, lockKey, content, resp.ETag)
	}

	return false, nil
}

// stealLock attempts to steal an expired lock using ETag for optimistic concurrency.
func (l *S3Locker) stealLock(ctx context.Context, lockKey, content string, etag *string) (bool, error) {
	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(l.bucket),
		Key:     aws.String(lockKey),
		Body:    bytes.NewReader([]byte(content)),
		IfMatch: etag,
	})
	return err == nil, nil
}

// Refresh updates the lock timestamp to extend ownership.
func (l *S3Locker) Refresh(ctx context.Context, groupID string, shardID int, consumerID string) error {
	lockKey := l.lockKey(groupID, shardID)
	now := time.Now().Format(time.RFC3339Nano)
	content := fmt.Sprintf("%s|%s", now, consumerID)

	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(lockKey),
		Body:   bytes.NewReader([]byte(content)),
	})
	return err
}

// Release deletes the lock, allowing other consumers to acquire it.
func (l *S3Locker) Release(ctx context.Context, groupID string, shardID int, consumerID string) error {
	lockKey := l.lockKey(groupID, shardID)
	_, err := l.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(lockKey),
	})
	return err
}

// lockKey generates the S3 key for a shard lock.
func (l *S3Locker) lockKey(groupID string, shardID int) string {
	return fmt.Sprintf("%s/locks/%s/shard-%d", l.queueName, groupID, shardID)
}
