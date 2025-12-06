package queue

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RetryManager tracks message retry attempts and enforces retry policies.
type RetryManager interface {
	Increment(ctx context.Context, groupID, messageID string) (int, error)
	Get(ctx context.Context, groupID, messageID string) (int, error)
	Delete(ctx context.Context, groupID, messageID string) error
	ShouldRetry(count int) bool
	ShouldMoveToDLQ(count int) bool
}

// S3RetryManager implements RetryManager using S3 objects.
type S3RetryManager struct {
	client     S3API
	bucket     string
	queueName  string
	maxRetries int
}

// NewS3RetryManager creates a new S3-based retry manager.
func NewS3RetryManager(client S3API, bucket, queueName string, maxRetries int) *S3RetryManager {
	return &S3RetryManager{
		client:     client,
		bucket:     bucket,
		queueName:  queueName,
		maxRetries: maxRetries,
	}
}

// Get retrieves the current retry count for a message.
// Returns 0 if no retry record exists.
func (r *S3RetryManager) Get(ctx context.Context, groupID, messageID string) (int, error) {
	key := r.retryKey(groupID, messageID)
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
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

// Increment increments the retry count for a message and returns the new count.
func (r *S3RetryManager) Increment(ctx context.Context, groupID, messageID string) (int, error) {
	count, err := r.Get(ctx, groupID, messageID)
	if err != nil {
		return 0, err
	}
	count++

	key := r.retryKey(groupID, messageID)
	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(fmt.Sprintf("%d", count))),
	})
	return count, err
}

// Delete removes the retry count record for a message.
func (r *S3RetryManager) Delete(ctx context.Context, groupID, messageID string) error {
	key := r.retryKey(groupID, messageID)
	_, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	return err
}

// ShouldRetry returns true if the message should be retried based on the count.
func (r *S3RetryManager) ShouldRetry(count int) bool {
	return count <= r.maxRetries
}

// ShouldMoveToDLQ returns true if the message should be moved to the DLQ.
func (r *S3RetryManager) ShouldMoveToDLQ(count int) bool {
	return count > r.maxRetries
}

// retryKey generates the S3 key for a retry count.
func (r *S3RetryManager) retryKey(groupID, messageID string) string {
	return fmt.Sprintf("%s/retries/%s/%s", r.queueName, groupID, messageID)
}
