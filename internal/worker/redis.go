package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/user_service/shared/tasks"
)

// StartRedisWorker listens to "queue:notifications"
func StartRedisWorker(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService) {
	log.Println("📥 Redis worker running (queue:notifications)...")
	for {
		select {
		case <-ctx.Done():
			log.Println("redis worker context done")
			return
		default:
			res, err := rdb.BRPop(ctx, 0*time.Second, "queue:notifications").Result()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("redis BRPop error: %v — retrying 1s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			if len(res) < 2 {
				continue
			}

			var msg tasks.NotificationPayload
			if err := json.Unmarshal([]byte(res[1]), &msg); err != nil {
				log.Printf("invalid JSON from redis: %v", err)
				continue
			}

			if err := services.SendEmailWithIdempotency(ctx, rdb, emailSvc, msg.Email, msg.Subject, msg.Body, []byte(res[1])); err != nil {
				log.Printf("failed sending queued email to %s: %v", msg.Email, err)
			}
		}
	}
}
