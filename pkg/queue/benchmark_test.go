package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkS3Queue_Publish(b *testing.B) {
	mockS3 := NewMockS3Client()
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:    "bench-bucket",
		QueueName: "bench-queue",
		Shards:    10,
	})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Publish(ctx, []byte("test"))
	}
}

func BenchmarkS3Queue_Consume(b *testing.B) {
	mockS3 := NewMockS3Client()
	// mockS3.latency = 50 * time.Millisecond // Set later
	q := NewS3Queue(mockS3, S3QueueConfig{
		Bucket:       "bench-bucket",
		QueueName:    "bench-queue",
		Shards:       1,
		PollInterval: 0,
	}) // Busy loop for max throughput
	ctx := context.Background()

	// Pre-populate (fast)
	for i := 0; i < b.N; i++ {
		q.Publish(ctx, []byte(fmt.Sprintf("msg-%d", i)))
	}

	mockS3.latency = 50 * time.Millisecond // Simulate realistic S3 latency

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(1)

	var count int64 // Changed to int64
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer wg.Done()
		q.Consume(ctx, "bench-group", func(ctx context.Context, msg Message) error {
			newCount := atomic.AddInt64(&count, 1) // Used atomic.AddInt64
			if newCount >= int64(b.N) {            // Updated condition
				cancel()
			}
			return nil
		})
	}()

	wg.Wait()
}
