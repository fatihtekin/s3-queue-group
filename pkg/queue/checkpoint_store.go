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

// CheckpointStore manages consumer group checkpoints.
type CheckpointStore interface {
	Get(ctx context.Context, groupID string, shardID int) (string, error)
	Set(ctx context.Context, groupID string, shardID int, messageID string) error
}

// S3CheckpointStore implements CheckpointStore using S3 objects.
type S3CheckpointStore struct {
	client    S3API
	bucket    string
	queueName string
}

// NewS3CheckpointStore creates a new S3-based checkpoint store.
func NewS3CheckpointStore(client S3API, bucket, queueName string) *S3CheckpointStore {
	return &S3CheckpointStore{
		client:    client,
		bucket:    bucket,
		queueName: queueName,
	}
}

// Get retrieves the last processed message ID for a consumer group and shard.
// Returns empty string if no checkpoint exists.
func (c *S3CheckpointStore) Get(ctx context.Context, groupID string, shardID int) (string, error) {
	key := c.checkpointKey(groupID, shardID)
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
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

// Set stores the last processed message ID for a consumer group and shard.
func (c *S3CheckpointStore) Set(ctx context.Context, groupID string, shardID int, messageID string) error {
	key := c.checkpointKey(groupID, shardID)
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(messageID)),
	})
	return err
}

// checkpointKey generates the S3 key for a checkpoint.
func (c *S3CheckpointStore) checkpointKey(groupID string, shardID int) string {
	return fmt.Sprintf("%s/checkpoints/%s/shard-%d", c.queueName, groupID, shardID)
}
