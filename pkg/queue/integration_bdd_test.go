package queue

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {
	// Cleanup any existing LocalStack containers before running tests
	if os.Getenv("TEST_INTEGRATION") == "1" {
		cleanupLocalStack()
	}
	os.Exit(m.Run())
}

func cleanupLocalStack() {
	// Best effort cleanup of existing localstack containers
	cmd := exec.Command("sh", "-c", "docker rm -f $(docker ps -q --filter ancestor=localstack/localstack:3.4.0)")
	_ = cmd.Run()
}

// BDDTest holds the state for the BDD test.
type BDDTest struct {
	t               *testing.T
	s3Client        S3API
	q               *S3Queue
	ctx             context.Context
	cancel          context.CancelFunc
	lastErr         error
	container       testcontainers.Container
	received        map[string][]Message
	consumerCancels map[string]context.CancelFunc
	mu              sync.Mutex
}

// Given step
type Given struct {
	test *BDDTest
}

// When step
type When struct {
	test *BDDTest
}

// Then step
type Then struct {
	test *BDDTest
}

// NewBDDTest initializes a new BDD test.
func NewBDDTest(t *testing.T) (*Given, *When, *Then) {
	test := &BDDTest{
		t:               t,
		received:        make(map[string][]Message),
		consumerCancels: make(map[string]context.CancelFunc),
	}
	test.ctx, test.cancel = context.WithTimeout(context.Background(), 120*time.Second)

	// Always use LocalStack for BDD tests
	cleanupLocalStack() // Ensure clean state before starting
	client, container, err := setupLocalStack(test.ctx)
	if err != nil {
		t.Fatalf("Failed to setup LocalStack: %v", err)
	}
	test.container = container
	test.s3Client = client
	test.q = NewS3Queue(client, S3QueueConfig{
		Bucket:       "test-bucket",
		QueueName:    "test-queue",
		Shards:       10,
		PollInterval: 10 * time.Millisecond,
		LockTTL:      2 * time.Second, // Short TTL for tests
	})

	// Create bucket
	_, err = client.CreateBucket(test.ctx, &s3.CreateBucketInput{
		Bucket: aws.String("test-bucket"),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Ensure container is terminated after test
	t.Cleanup(func() {
		if test.container != nil {
			// Use a fresh context for cleanup as test context might be cancelled
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := test.container.Terminate(cleanupCtx); err != nil {
				t.Logf("Failed to terminate container: %v", err)
			}
		}
	})

	// Cleanup (for context cancellation)
	t.Cleanup(func() {
		test.cancel()
	})

	return &Given{test}, &When{test}, &Then{test}
}

func setupLocalStack(ctx context.Context) (*s3.Client, testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:3.4.0",
		ExposedPorts: []string{"4566/tcp"},
		WaitingFor:   wait.ForLog("Ready."),
		Env: map[string]string{
			"SERVICES": "s3",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start localstack: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get host: %w", err)
	}
	port, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get port: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		// Disable checksum validation to avoid "Response has no supported checksum" warnings with LocalStack
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationUnset
	})

	return client, container, nil
}

// --- Given ---

func (g *Given) a_queue_with_shards(n int) *Given {
	// Ensure bucket exists if using real S3
	if _, ok := g.test.s3Client.(*MockS3Client); !ok {
		_, err := g.test.s3Client.CreateBucket(g.test.ctx, &s3.CreateBucketInput{
			Bucket: aws.String("bdd-bucket"),
		})
		if err != nil {
			// Ignore if exists? LocalStack starts empty usually.
			// But if we reuse container it might exist.
			// For now, fail if error unless it's "BucketAlreadyOwnedByYou" which is hard to check portably.
			// Just log error.
			g.test.t.Logf("CreateBucket error (might be ok): %v", err)
		}
	}

	g.test.q = NewS3Queue(g.test.s3Client, S3QueueConfig{
		Bucket:       "bdd-bucket",
		QueueName:    "bdd-queue",
		Shards:       n,
		PollInterval: 10 * time.Millisecond,
	})
	return g
}

func (g *Given) and() *Given {
	return g
}

// --- When ---

func (w *When) a_message_is_published(body string) *When {
	err := w.test.q.Publish(w.test.ctx, []byte(body))
	if err != nil {
		w.test.lastErr = err
	}
	return w
}

func (w *When) a_consumer_group_consumes_with_count(group string, count int) *When {
	for i := 0; i < count; i++ {
		go func() {
			err := w.test.q.Consume(w.test.ctx, group, func(ctx context.Context, msg Message) error {
				w.test.mu.Lock()
				w.test.received[group] = append(w.test.received[group], msg)
				w.test.mu.Unlock()
				return nil
			})
			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				w.test.t.Errorf("Consume error: %v", err)
			}
		}()
	}
	// Give it some time to process
	time.Sleep(100 * time.Millisecond)
	return w
}

func (w *When) a_consumer_group_consumes(group string) *When {
	// Default to one consumer per shard
	return w.a_consumer_group_consumes_with_count(group, w.test.q.cfg.Shards)
}

func (w *When) a_consumer_named_consumes(name, group string) *When {
	ctx, cancel := context.WithCancel(w.test.ctx)
	w.test.mu.Lock()
	w.test.consumerCancels[name] = cancel
	w.test.mu.Unlock()

	go func() {
		err := w.test.q.Consume(ctx, group, func(ctx context.Context, msg Message) error {
			w.test.mu.Lock()
			w.test.received[group] = append(w.test.received[group], msg)
			w.test.mu.Unlock()
			return nil
		})
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			w.test.t.Errorf("Consume error: %v", err)
		}
	}()
	// Give it some time to start
	time.Sleep(100 * time.Millisecond)
	return w
}

func (w *When) the_consumer_dies(name string) *When {
	w.test.mu.Lock()
	cancel, ok := w.test.consumerCancels[name]
	w.test.mu.Unlock()

	if ok {
		cancel()
		// Give it time to stop
		time.Sleep(100 * time.Millisecond)
	} else {
		w.test.t.Errorf("Consumer %q not found", name)
	}
	return w
}

func (w *When) we_wait_for_lock_expiration() *When {
	// LockTTL is 2s in tests, wait 3s to be safe
	time.Sleep(3 * time.Second)
	return w
}

func (w *When) and() *When {
	return w
}

// --- Then ---

func (t *Then) the_message_should_be_received(body string) *Then {
	// Check if ANY group received it
	start := time.Now()
	found := false
	for time.Since(start) < 30*time.Second {
		t.test.mu.Lock()
		for _, msgs := range t.test.received {
			for _, msg := range msgs {
				if string(msg.Data) == body {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		t.test.mu.Unlock()

		if found {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !found {
		t.test.t.Errorf("Expected message %q to be received by at least one group", body)
	}
	return t
}

func (t *Then) the_message_should_be_received_by_group(body, group string) *Then {
	start := time.Now()
	found := false
	for time.Since(start) < 30*time.Second {
		t.test.mu.Lock()
		msgs := t.test.received[group]
		for _, msg := range msgs {
			if string(msg.Data) == body {
				found = true
				break
			}
		}
		t.test.mu.Unlock()

		if found {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !found {
		t.test.t.Errorf("Expected message %q to be received by group %q", body, group)
	}
	return t
}

func (t *Then) messages_should_be_distributed_across_shards(minShards int) *Then {
	start := time.Now()
	uniqueShards := make(map[int]bool)

	for time.Since(start) < 30*time.Second {
		t.test.mu.Lock()
		for _, msgs := range t.test.received {
			for _, msg := range msgs {
				uniqueShards[msg.ShardID] = true
			}
		}
		t.test.mu.Unlock()

		if len(uniqueShards) >= minShards {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(uniqueShards) < minShards {
		t.test.t.Errorf("Expected messages to be distributed across at least %d shards, but got %d", minShards, len(uniqueShards))
	}
	return t
}

func (t *Then) no_error_should_occur() *Then {
	if t.test.lastErr != nil {
		t.test.t.Errorf("Unexpected error: %v", t.test.lastErr)
	}
	return t
}

func (t *Then) and() *Then {
	return t
}

// --- Scenarios ---

func TestAcc_PublishAndConsume(t *testing.T) {
	given, when, then := NewBDDTest(t)

	given.
		a_queue_with_shards(1)

	when.
		a_message_is_published("hello").and().
		a_consumer_group_consumes("group-1")

	then.
		the_message_should_be_received("hello").and().
		no_error_should_occur()
}

func TestAcc_FanOut(t *testing.T) {
	given, when, then := NewBDDTest(t)

	given.
		a_queue_with_shards(1)

	when.
		a_message_is_published("broadcast").and().
		a_consumer_group_consumes("group-a").and().
		a_consumer_group_consumes("group-b")

	then.
		the_message_should_be_received_by_group("broadcast", "group-a").and().
		the_message_should_be_received_by_group("broadcast", "group-b").and().
		no_error_should_occur()
}

func TestAcc_MultipleShards(t *testing.T) {
	given, when, then := NewBDDTest(t)

	given.
		a_queue_with_shards(2)

	for i := 0; i < 10; i++ {
		when.a_message_is_published(fmt.Sprintf("msg-%d", i))
	}

	when.
		a_consumer_group_consumes("group-multi")

	for i := 0; i < 10; i++ {
		then.the_message_should_be_received_by_group(fmt.Sprintf("msg-%d", i), "group-multi")
	}

	then.
		messages_should_be_distributed_across_shards(2).and().
		no_error_should_occur()
}

func TestAcc_SingleConsumerMultipleShards(t *testing.T) {
	given, when, then := NewBDDTest(t)

	given.
		a_queue_with_shards(2)

	for i := 0; i < 10; i++ {
		when.a_message_is_published(fmt.Sprintf("msg-%d", i))
	}

	when.
		a_consumer_group_consumes_with_count("group-single", 1)

	for i := 0; i < 10; i++ {
		then.the_message_should_be_received_by_group(fmt.Sprintf("msg-%d", i), "group-single")
	}

	then.
		messages_should_be_distributed_across_shards(2).and().
		no_error_should_occur()
}

func TestAcc_MoreConsumersThanShards(t *testing.T) {
	given, when, then := NewBDDTest(t)

	given.
		a_queue_with_shards(2)

	for i := 0; i < 10; i++ {
		when.a_message_is_published(fmt.Sprintf("msg-%d", i))
	}

	when.
		// Start 4 consumers for 2 shards
		a_consumer_group_consumes_with_count("group-surplus", 4)

	for i := 0; i < 10; i++ {
		then.the_message_should_be_received_by_group(fmt.Sprintf("msg-%d", i), "group-surplus")
	}

	then.
		messages_should_be_distributed_across_shards(2).and().
		no_error_should_occur()
}

func TestAcc_ConsumerFailover(t *testing.T) {
	given, when, then := NewBDDTest(t)

	given.
		a_queue_with_shards(1)

	when.
		// Start consumer A first, it should grab the lock
		a_consumer_named_consumes("consumer-A", "group-failover").and().
		// Start consumer B, it should wait
		a_consumer_named_consumes("consumer-B", "group-failover").and().
		a_message_is_published("msg-1")

	then.
		the_message_should_be_received_by_group("msg-1", "group-failover")

	when.
		the_consumer_dies("consumer-A").and().
		we_wait_for_lock_expiration().and().
		a_message_is_published("msg-2")

	then.
		the_message_should_be_received_by_group("msg-2", "group-failover").and().
		no_error_should_occur()
}
