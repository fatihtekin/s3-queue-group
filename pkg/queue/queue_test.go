package queue

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MockObject represents an object in S3.
type MockObject struct {
	Data         []byte
	LastModified time.Time
}

// MockS3Client simulates S3 behavior in memory.
type MockS3Client struct {
	mu      sync.Mutex
	objects map[string]MockObject
	latency time.Duration
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects: make(map[string]MockObject),
	}
}

func (m *MockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	// fmt.Printf("PutObject latency: %v\n", m.latency)
	time.Sleep(m.latency)
	m.mu.Lock()
	defer m.mu.Unlock()

	key := *params.Key

	// Simulate IfNoneMatch: *
	if params.IfNoneMatch != nil && *params.IfNoneMatch == "*" {
		if _, exists := m.objects[key]; exists {
			return nil, fmt.Errorf("PreconditionFailed")
		}
	}

	// Simulate IfMatch
	if params.IfMatch != nil {
		obj, exists := m.objects[key]
		if !exists {
			return nil, &types.NoSuchKey{}
		}
		// Simple ETag: hash of content
		etag := fmt.Sprintf(`"%x"`, len(obj.Data))
		if *params.IfMatch != etag {
			return nil, fmt.Errorf("PreconditionFailed")
		}
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(params.Body)
	m.objects[key] = MockObject{
		Data:         buf.Bytes(),
		LastModified: time.Now(),
	}

	return &s3.PutObjectOutput{}, nil
}

func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	time.Sleep(m.latency)
	m.mu.Lock()
	defer m.mu.Unlock()

	key := *params.Key
	obj, exists := m.objects[key]
	if !exists {
		return nil, &types.NoSuchKey{}
	}

	etag := fmt.Sprintf(`"%x"`, len(obj.Data))

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(obj.Data)),
		ETag: aws.String(etag),
	}, nil
}

func (m *MockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	time.Sleep(m.latency)
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, *params.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *MockS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	time.Sleep(m.latency)
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, obj := range params.Delete.Objects {
		delete(m.objects, *obj.Key)
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (m *MockS3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	time.Sleep(m.latency)
	m.mu.Lock()
	defer m.mu.Unlock()

	source := *params.CopySource
	parts := strings.SplitN(source, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid copy source")
	}
	sourceKey := parts[1]

	obj, exists := m.objects[sourceKey]
	if !exists {
		return nil, &types.NoSuchKey{}
	}

	m.objects[*params.Key] = MockObject{
		Data:         append([]byte(nil), obj.Data...),
		LastModified: time.Now(),
	}
	return &s3.CopyObjectOutput{}, nil
}

func (m *MockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.latency > 0 {
	}
	time.Sleep(m.latency)
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix := ""
	if params.Prefix != nil {
		prefix = *params.Prefix
	}

	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	startAfter := ""
	if params.StartAfter != nil {
		startAfter = *params.StartAfter
	}

	var contents []types.Object
	count := int32(0)
	limit := int32(1000)
	if params.MaxKeys != nil {
		limit = *params.MaxKeys
	}

	for _, k := range keys {
		if startAfter != "" && k <= startAfter {
			continue
		}
		if count >= limit {
			break
		}
		obj := m.objects[k]
		k := k // copy
		contents = append(contents, types.Object{
			Key:          &k,
			LastModified: &obj.LastModified,
		})
		count++
	}

	return &s3.ListObjectsV2Output{
		Contents: contents,
	}, nil
}

func (m *MockS3Client) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return &s3.CreateBucketOutput{}, nil
}

func TestS3Queue_Integration(t *testing.T) {
	mockS3 := NewMockS3Client()
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:       "test-bucket",
		QueueName:    "test-queue",
		Shards:       2,
		PollInterval: 10 * time.Millisecond,
	}) // Fast poll for test

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Publish messages
	msgCount := 50
	for i := 0; i < msgCount; i++ {
		err := q.Publish(ctx, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	// 2. Start consumers for Group A
	consumerCount := 2
	var wg sync.WaitGroup
	processedA := make(map[string]int)
	var processedAMu sync.Mutex

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := q.Consume(ctx, "group-a", func(ctx context.Context, msg Message) error {
				processedAMu.Lock()
				processedA[string(msg.Data)]++
				processedAMu.Unlock()
				return nil
			})
			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				t.Errorf("Consumer A-%d error: %v", id, err)
			}
		}(i)
	}

	// 3. Start consumers for Group B (Fan-out)
	processedB := make(map[string]int)
	var processedBMu sync.Mutex
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := q.Consume(ctx, "group-b", func(ctx context.Context, msg Message) error {
				processedBMu.Lock()
				processedB[string(msg.Data)]++
				processedBMu.Unlock()
				return nil
			})
			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				t.Errorf("Consumer B-%d error: %v", id, err)
			}
		}(i)
	}

	// Wait for processing (or timeout)
	// We need a way to stop consumers when done.
	// For this test, we'll just wait a bit and check results, or wait until all processed.

	// Better: wait loop
	// Wait loop
	start := time.Now()
	for {
		processedAMu.Lock()
		countA := len(processedA)
		processedAMu.Unlock()

		processedBMu.Lock()
		countB := len(processedB)
		processedBMu.Unlock()

		if countA == msgCount && countB == msgCount {
			break
		}
		if time.Since(start) > 4*time.Second {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel() // Stop consumers
	wg.Wait()

	// 3. Verify
	processedAMu.Lock()
	defer processedAMu.Unlock()
	if len(processedA) != msgCount {
		t.Errorf("Group A: Expected %d processed messages, got %d", msgCount, len(processedA))
	}

	processedBMu.Lock()
	defer processedBMu.Unlock()
	if len(processedB) != msgCount {
		t.Errorf("Group B: Expected %d processed messages, got %d", msgCount, len(processedB))
	}
}

func TestLockExpiration(t *testing.T) {
	mockS3 := NewMockS3Client()
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:    "test-bucket",
		QueueName: "test-queue",
		Shards:    1,
		LockTTL:   100 * time.Millisecond,
	})

	ctx := context.Background()
	groupID := "group-1"
	shardID := 0

	// 1. Acquire lock manually
	if !q.tryLockShard(ctx, groupID, shardID, "test-consumer") {
		t.Fatal("Failed to acquire lock")
	}

	// 2. Try to acquire again immediately (should fail)
	if q.tryLockShard(ctx, groupID, shardID, "test-consumer-2") {
		t.Fatal("Should not be able to acquire lock immediately")
	}

	// 3. Wait for TTL
	time.Sleep(150 * time.Millisecond)

	// 4. Try to acquire again (should succeed via stealing)
	if !q.tryLockShard(ctx, groupID, shardID, "test-consumer") {
		t.Fatal("Failed to acquire expired lock")
	}
}

func TestHeartbeat(t *testing.T) {
	mockS3 := NewMockS3Client()
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:    "test-bucket",
		QueueName: "test-queue",
		Shards:    1,
		LockTTL:   200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	groupID := "group-1"
	shardID := 0

	// 1. Start heartbeat
	go q.heartbeat(ctx, cancel, groupID, shardID, "test-consumer")

	// 2. Wait longer than TTL (heartbeat should keep it alive)
	time.Sleep(500 * time.Millisecond)

	// 3. Check lock timestamp
	lockKey := fmt.Sprintf("test-queue/locks/%s/shard-%d", groupID, shardID)
	resp, err := mockS3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String(lockKey),
	})
	if err != nil {
		t.Fatalf("Failed to get lock: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	parts := strings.Split(string(data), "|")
	ts, _ := time.Parse(time.RFC3339Nano, parts[0])

	if time.Since(ts) > 200*time.Millisecond {
		t.Errorf("Lock timestamp is too old: %v", ts)
	}
}

func TestDLQ(t *testing.T) {
	mockS3 := NewMockS3Client()
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:       "test-bucket",
		QueueName:    "test-queue",
		Shards:       1,
		PollInterval: 10 * time.Millisecond,
		MaxRetries:   2,
		DLQName:      "test-queue-dlq",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Publish a message
	q.Publish(ctx, []byte("fail-me"))

	// 2. Consume with a handler that always fails
	err := q.Consume(ctx, "group-dlq", func(ctx context.Context, msg Message) error {
		if string(msg.Data) == "fail-me" {
			return fmt.Errorf("simulated failure")
		}
		return nil
	})

	// Consume returns error when ctx is canceled or fatal error.
	// Here we expect it to run until timeout (ctx canceled) because it should handle the error via DLQ and continue (but there are no more messages).
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("Consume returned unexpected error: %v", err)
	}

	// 3. Verify message is in DLQ
	// We need to list objects in DLQ
	resp, err := mockS3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("test-bucket"),
		Prefix: aws.String("test-queue-dlq/"),
	})
	if err != nil {
		t.Fatalf("Failed to list DLQ: %v", err)
	}
	if len(resp.Contents) != 1 {
		t.Errorf("Expected 1 message in DLQ, got %d", len(resp.Contents))
	}
}

func TestCleanup(t *testing.T) {
	mockS3 := NewMockS3Client()
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:    "test-bucket",
		QueueName: "test-queue",
		Shards:    1,
	})

	ctx := context.Background()

	// 1. Publish "old" message
	q.Publish(ctx, []byte("old"))

	// 2. Publish "new" message
	q.Publish(ctx, []byte("new"))

	// 3. Manually modify LastModified in mock
	mockS3.mu.Lock()
	for k, obj := range mockS3.objects {
		// Identify old message by content (not ideal but works for test)
		if string(obj.Data) == "old" {
			obj.LastModified = time.Now().Add(-24 * time.Hour)
			mockS3.objects[k] = obj
		}
	}
	mockS3.mu.Unlock()

	// 4. Run Cleanup
	if err := q.Cleanup(ctx, 1*time.Hour); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// 5. Verify
	mockS3.mu.Lock()
	defer mockS3.mu.Unlock()

	count := 0
	for _, obj := range mockS3.objects {
		if strings.Contains(string(obj.Data), "old") {
			t.Errorf("Old message should have been deleted")
		}
		if strings.Contains(string(obj.Data), "new") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 new message, got %d", count)
	}
}

type MockMetricsReporter struct {
	Published int
	Consumed  int
	Errors    int
	DLQ       int
}

func (m *MockMetricsReporter) IncPublished()                             { m.Published++ }
func (m *MockMetricsReporter) IncConsumed()                              { m.Consumed++ }
func (m *MockMetricsReporter) IncError(op string)                        { m.Errors++ }
func (m *MockMetricsReporter) IncDLQ()                                   { m.DLQ++ }
func (m *MockMetricsReporter) ObserveLatency(op string, d time.Duration) {}

func TestMetrics(t *testing.T) {
	mockS3 := NewMockS3Client()
	metrics := &MockMetricsReporter{}
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:       "test-bucket",
		QueueName:    "test-queue",
		Shards:       1,
		PollInterval: 10 * time.Millisecond,
		Metrics:      metrics,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Publish
	q.Publish(ctx, []byte("msg"))
	if metrics.Published != 1 {
		t.Errorf("Expected 1 published, got %d", metrics.Published)
	}

	// 2. Consume
	q.Consume(ctx, "group-metrics", func(ctx context.Context, msg Message) error {
		return nil
	})

	// Wait for consume
	// Since Consume blocks, we rely on timeout or we should run it in goroutine.
	// But here we ran it in main thread and it returned on timeout.
	// So we check metrics after.
	if metrics.Consumed != 1 {
		t.Errorf("Expected 1 consumed, got %d", metrics.Consumed)
	}
}
