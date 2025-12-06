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

// MessageRef represents a reference to a message in storage.
type MessageRef struct {
	Key       string
	MessageID string
}

// MessageStore handles message storage and retrieval.
type MessageStore interface {
	Publish(ctx context.Context, shardID int, messageID string, data []byte) error
	Fetch(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, shardID int, startAfter string, maxKeys int) ([]MessageRef, error)
	MoveToDLQ(ctx context.Context, key, messageID, dlqName string) error
}

// S3MessageStore implements MessageStore using S3 objects.
type S3MessageStore struct {
	client    S3API
	bucket    string
	queueName string
}

// NewS3MessageStore creates a new S3-based message store.
func NewS3MessageStore(client S3API, bucket, queueName string) *S3MessageStore {
	return &S3MessageStore{
		client:    client,
		bucket:    bucket,
		queueName: queueName,
	}
}

// Publish stores a message in the specified shard.
func (m *S3MessageStore) Publish(ctx context.Context, shardID int, messageID string, data []byte) error {
	key := m.messageKey(shardID, messageID)
	_, err := m.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// Fetch retrieves a message by its key.
func (m *S3MessageStore) Fetch(ctx context.Context, key string) ([]byte, error) {
	resp, err := m.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	return data, nil
}

// List returns message references from a shard, starting after the specified key.
func (m *S3MessageStore) List(ctx context.Context, shardID int, startAfter string, maxKeys int) ([]MessageRef, error) {
	prefix := m.shardPrefix(shardID)
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(m.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(maxKeys)),
	}
	if startAfter != "" {
		params.StartAfter = aws.String(startAfter)
	}

	resp, err := m.client.ListObjectsV2(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to list messages: %w", err)
	}

	refs := make([]MessageRef, 0, len(resp.Contents))
	for _, obj := range resp.Contents {
		key := *obj.Key
		parts := strings.Split(key, "/")
		messageID := parts[len(parts)-1]
		refs = append(refs, MessageRef{
			Key:       key,
			MessageID: messageID,
		})
	}
	return refs, nil
}

// MoveToDLQ copies a message to the dead-letter queue.
func (m *S3MessageStore) MoveToDLQ(ctx context.Context, key, messageID, dlqName string) error {
	if dlqName == "" {
		return fmt.Errorf("DLQ not configured")
	}

	dlqKey := fmt.Sprintf("%s/%s", dlqName, messageID)
	_, err := m.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(m.bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", m.bucket, key)),
		Key:        aws.String(dlqKey),
	})
	if err != nil {
		return fmt.Errorf("failed to copy to DLQ: %w", err)
	}
	return nil
}

// messageKey generates the S3 key for a message.
func (m *S3MessageStore) messageKey(shardID int, messageID string) string {
	return fmt.Sprintf("%s/topic/shard-%d/%s", m.queueName, shardID, messageID)
}

// shardPrefix generates the S3 prefix for a shard.
func (m *S3MessageStore) shardPrefix(shardID int) string {
	return fmt.Sprintf("%s/topic/shard-%d/", m.queueName, shardID)
}
