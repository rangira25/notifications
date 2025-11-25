package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/rangira25/notification/internal/kafka"
	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/notification/internal/storage"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("worker: .env file not found, using environment variables")
	}

	// Redis for idempotency and app features
	redisAddr := os.Getenv("REDIS_ADDR")
	redisPass := os.Getenv("REDIS_PASSWORD")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := storage.NewRedis(redisAddr, redisPass)

	// Read SMTP config from env
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASS")
	if smtpHost == "" || smtpPort == "" || smtpUser == "" || smtpPass == "" {
		log.Fatal("missing SMTP_* env vars")
	}
	emailSvc := services.NewEmailService(smtpHost, smtpPort, smtpUser, smtpPass)

	// Kafka config
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "185.***.***.***:9092" // fallback
	}
	brokers := []string{kafkaBroker}
	topic := "notifications"

	fmt.Println("starting kafka consumer with idempotency via redis")
	kafka.StartConsumerWithHandler(brokers, topic, func(msgBytes []byte) {
		// compute message fingerprint for idempotency
		sum := sha256.Sum256(msgBytes)
		key := "kafka:msg:" + hex.EncodeToString(sum[:])

		// Try setnx (only process if not already processed)
		// Use 24h TTL to avoid reprocessing same message within that window
		set, err := rdb.SetNX(context.Background(), key, "1", 24*time.Hour).Result()
		if err != nil {
			// Redis error -> log and continue (we might reprocess)
			log.Printf("redis SetNX error: %v - will attempt to process message", err)
		}
		if !set {
			log.Printf("skipping already-processed message (key=%s)", key)
			return
		}

		// Unmarshal structured JSON
		var msg kafka.NotificationMessage
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			log.Printf("invalid kafka json: %v", err)
			return
		}

		// Send email with a context deadline
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		log.Printf("sending email to %s (subject=%s)", msg.Email, msg.Subject)
		if err := emailSvc.SendEmail(ctx, msg.Email, msg.Subject, msg.Body); err != nil {
			// If email fails, you might want to remove the idempotency key so it can retry:
			// rdb.Del(context.Background(), key)
			log.Printf("email send error: %v", err)
			return
		}
		log.Printf("email sent to %s", msg.Email)
	})

	select {}
}
