package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Cleanup deletes messages older than the retention period.
// Note: This method requires direct S3 access for batch deletion efficiency.
func (q *S3Queue) Cleanup(ctx context.Context, client S3API, retention time.Duration) error {
	cutoff := time.Now().Add(-retention)
	log.Printf("Starting cleanup for messages older than %v", cutoff)

	for i := 0; i < q.cfg.Shards; i++ {
		prefix := fmt.Sprintf("%s/topic/shard-%d/", q.cfg.QueueName, i)
		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(q.cfg.Bucket),
			Prefix: aws.String(prefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to list objects for cleanup: %w", err)
			}

			var toDelete []types.ObjectIdentifier
			for _, obj := range page.Contents {
				if obj.LastModified.Before(cutoff) {
					toDelete = append(toDelete, types.ObjectIdentifier{Key: obj.Key})
				}
			}

			if len(toDelete) > 0 {
				log.Printf("Deleting %d old messages from shard %d", len(toDelete), i)
				_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(q.cfg.Bucket),
					Delete: &types.Delete{
						Objects: toDelete,
						Quiet:   aws.Bool(true),
					},
				})
				if err != nil {
					log.Printf("Failed to delete batch: %v", err)
					// Continue best effort
				}
			}
		}
	}
	return nil
}
