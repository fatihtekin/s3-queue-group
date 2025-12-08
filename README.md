# S3 Brokerless Queue Group

A distributed, log-structured message queue implementation using Amazon S3 as the storage backend. This system provides reliable message delivery with consumer groups, automatic failover, retries, and dead-letter queue (DLQ) support.

## Features
- No need to maintain/run broker infrastructure
- **Distributed Architecture**: Multiple producers and consumers can operate concurrently
- **Consumer Groups**: Messages are processed exactly once per consumer group
- **Sharding**: Horizontal scalability through configurable sharding
- **Automatic Failover**: Consumers automatically take over failed shards using distributed locking
- **Retry Mechanism**: Configurable retry attempts with automatic DLQ routing for failed messages
- **Checkpointing**: Per-shard checkpoints ensure no message loss and enable resumption
- **Metrics**: Built-in metrics reporting for monitoring queue health
- **Lock-based Coordination**: TTL-based distributed locks prevent duplicate processing

## Architecture

### Core Components

```
┌─────────────┐         ┌─────────────┐
│  Producer   │────────▶│     S3      │
└─────────────┘         │   Bucket    │
                        │             │
┌─────────────┐         │  ┌────────┐ │
│  Consumer   │◀────────│  │ Shard  │ │
│   Group A   │         │  │   0    │ │
└─────────────┘         │  ├────────┤ │
                        │  │ Shard  │ │
┌─────────────┐         │  │   1    │ │
│  Consumer   │◀────────│  ├────────┤ │
│   Group B   │         │  │  ...   │ │
└─────────────┘         │  └────────┘ │
                        └─────────────┘
```

### S3 Object Structure

```
bucket/
├── {queue-name}/
│   ├── topic/
│   │   ├── shard-0/
│   │   │   ├── {timestamp}_{uuid}
│   │   │   └── ...
│   │   ├── shard-1/
│   │   │   └── ...
│   │   └── ...
│   ├── locks/
│   │   └── {group-id}/
│   │       ├── shard-0
│   │       └── ...
│   ├── checkpoints/
│   │   └── {group-id}/
│   │       ├── shard-0
│   │       └── ...
│   ├── retries/
│   │   └── {group-id}/
│   │       └── {message-id}
│   └── dlq/
│       └── {message-id}
```

## Installation

```bash
go get s3-queue
```

## Prerequisites

- Go 1.25.5 or later
- AWS credentials configured (via environment variables, AWS config file, or IAM role)
- An S3 bucket with appropriate permissions

### Required S3 Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:CopyObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name/*",
        "arn:aws:s3:::your-bucket-name"
      ]
    }
  ]
}
```

## Usage

### Producer Example

```go
package main

import (
    "context"
    "log"
    
    "s3-queue/pkg/queue"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
    ctx := context.Background()
    
    // Load AWS config
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    // Create S3 client
    client := s3.NewFromConfig(cfg)
    
    // Create queue
    q := queue.NewS3Queue(client, queue.S3QueueConfig{
        Bucket:       "my-bucket",
        QueueName:    "my-queue",
        Shards:       10,
        PollInterval: 1 * time.Second,
        LockTTL:      30 * time.Second,
        MaxRetries:   3,
        DLQName:      "my-dlq",
    })
    
    // Publish messages
    err = q.Publish(ctx, []byte("Hello, World!"))
    if err != nil {
        log.Fatal(err)
    }
}
```

### Consumer Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "s3-queue/pkg/queue"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
    ctx := context.Background()
    
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    client := s3.NewFromConfig(cfg)
    
    q := queue.NewS3Queue(client, queue.S3QueueConfig{
        Bucket:       "my-bucket",
        QueueName:    "my-queue",
        Shards:       10,
        PollInterval: 1 * time.Second,
        LockTTL:      30 * time.Second,
        MaxRetries:   3,
        DLQName:      "my-dlq",
    })
    
    // Start consuming
    err = q.Consume(ctx, "my-consumer-group", func(ctx context.Context, msg queue.Message) error {
        fmt.Printf("Received: %s (Shard: %d)\n", string(msg.Data), msg.ShardID)
        // Process message here
        return nil
    })
    
    if err != nil {
        log.Fatal(err)
    }
}
```

### Running the Examples

**Start a producer:**
```bash
go run cmd/producer/main.go -bucket my-bucket -queue my-queue -count 100
```

**Start consumers:**
```bash
# Terminal 1
go run cmd/consumer/main.go -bucket my-bucket -queue my-queue

# Terminal 2 (for testing failover)
go run cmd/consumer/main.go -bucket my-bucket -queue my-queue
```

## Configuration

### S3QueueConfig Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Bucket` | `string` | *required* | S3 bucket name |
| `QueueName` | `string` | *required* | Logical queue name (prefix for S3 keys) |
| `Shards` | `int` | `1` | Number of shards for parallelism |
| `PollInterval` | `time.Duration` | `1s` | Interval between polling attempts |
| `LockTTL` | `time.Duration` | `30s` | Lock expiration time for shard ownership |
| `MaxRetries` | `int` | `3` | Maximum retry attempts before DLQ |
| `DLQName` | `string` | `""` | Dead-letter queue name (optional) |
| `Metrics` | `MetricsReporter` | `NoopMetricsReporter` | Custom metrics reporter |

## How It Works

### Message Publishing

1. Producer generates a unique message ID with timestamp and UUID
2. Message is hashed to determine target shard
3. Message is written to S3 at `{queue}/topic/shard-{N}/{id}`

### Message Consumption

1. Consumer attempts to acquire lock on a shard
2. If successful, reads checkpoint to determine last processed message
3. Lists messages in shard starting after checkpoint
4. Processes messages sequentially
5. Updates checkpoint after successful processing
6. Sends heartbeats to maintain lock ownership
7. On failure, increments retry counter
8. After max retries, moves message to DLQ

### Distributed Locking

- Locks are stored as S3 objects containing `{timestamp}|{consumer-id}`
- Lock acquisition uses S3's `IfNoneMatch: "*"` for atomic creation
- Expired locks can be stolen using `IfMatch` with ETag for optimistic concurrency
- Heartbeat goroutine refreshes lock every `LockTTL/2`
- Lost locks trigger immediate processing cancellation

### Failover Mechanism

When a consumer dies:
1. Its lock expires after `LockTTL`
2. Another consumer detects the expired lock
3. New consumer steals the lock using ETag-based optimistic locking
4. Processing resumes from the last checkpoint
5. No messages are lost or duplicated

## Testing

The project includes comprehensive tests:

```bash
# Run unit tests
go test ./pkg/queue/...

# Run integration tests (requires LocalStack)
go test ./pkg/queue/... -tags=integration

# Run benchmarks
go test -bench=. ./pkg/queue/...
```

### BDD Integration Tests

The project includes behavior-driven development tests that verify:
- Multiple consumers processing different shards
- Consumer failover when a consumer dies
- Exactly-once processing per consumer group
- Checkpoint persistence and recovery

## Metrics

Implement the `MetricsReporter` interface to collect metrics:

```go
type MetricsReporter interface {
    IncPublished()
    IncConsumed()
    IncDLQ()
    IncError(operation string)
    ObserveLatency(operation string, duration time.Duration)
}
```

Example metrics to track:
- Messages published/consumed per second
- Processing latency
- DLQ message count
- Lock acquisition failures
- Retry counts

## Performance Considerations

- **Sharding**: More shards = more parallelism, but also more S3 API calls
- **Batch Size**: `ListObjectsV2` fetches up to 100 messages per call
- **Lock TTL**: Shorter TTL = faster failover, but more heartbeat overhead
- **Poll Interval**: Balance between latency and S3 API costs

### Recommended Settings

For high-throughput workloads:
```go
S3QueueConfig{
    Shards:       100,
    PollInterval: 100 * time.Millisecond,
    LockTTL:      10 * time.Second,
}
```

For cost-optimized workloads:
```go
S3QueueConfig{
    Shards:       10,
    PollInterval: 5 * time.Second,
    LockTTL:      60 * time.Second,
}
```

## Limitations

- **Steady-State Performance**: Once a consumer has a shard lock and is processing messages:
  - **Per-message latency**: Sub-second (often sub-millisecond) within batches
  - **Throughput**: Hundreds to thousands of messages per second per consumer
  - Messages are fetched in batches of up to 100 via `ListObjectsV2`
- **Cold Start**: Initial lock acquisition and first message fetch takes 200ms-1.5s
- **API Rate Limits**: Subject to S3 rate limits (3,500 PUT/s, 5,500 GET/s per prefix)
- **Ordering**: Messages are ordered within a shard, but not across shards
- **Consistency**: S3's eventual consistency may cause brief inconsistencies

## Use Cases

This queue excels at:
- ✅ **High-throughput batch processing** (thousands of messages/second once consuming)
- ✅ Asynchronous job processing and task queues
- ✅ Event-driven architectures with multiple consumer groups
- ✅ Data pipeline coordination and workflow orchestration
- ✅ Audit log processing and compliance workflows
- ✅ ETL and data transformation pipelines
- ✅ Distributed systems requiring exactly-once processing per consumer group

**Performance Profile**: Optimized for sustained high-throughput workloads where consumers continuously process messages. The batch-oriented design means once a consumer starts processing, it achieves excellent per-message latency and throughput.

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- Code follows Go conventions
- New features include tests
- Documentation is updated

## License

MIT License - see LICENSE file for details

## Acknowledgments

Built with:
- [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)
- [testcontainers-go](https://github.com/testcontainers/testcontainers-go) for integration testing
