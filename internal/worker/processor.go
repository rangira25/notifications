package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/user_service/shared/tasks"
)

type kafkaEvent struct {
	// support both styles:
	// 1) generic notification: { "email","subject","body" }
	// 2) typed event: { "type": "UserRegistered", "email": "...", "name": "...", "otp": "..." }
	Type    string `json:"type,omitempty"`
	Email   string `json:"email,omitempty"`
	Subject string `json:"subject,omitempty"`
	Body    string `json:"body,omitempty"`
	Name    string `json:"name,omitempty"`
	Otp     string `json:"otp,omitempty"`
	Code    string `json:"code,omitempty"`
}

func main() {
	_ = godotenv.Load()

	// ENV
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	redisPass := os.Getenv("REDIS_PASSWORD")
	kafkaBrokersEnv := getenv("KAFKA_BROKERS", os.Getenv("KAFKA_BROKER"))
	kafkaTopic := getenv("KAFKA_TOPIC", "auth.events")

	// SMTP
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASS")

	// Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
		DB:       0,
	})

	// Email service
	emailSvc := services.NewEmailService(smtpHost, smtpPort, smtpUser, smtpPass)

	// Context + shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Redis worker (process jobs pushed directly into queue:notifications)
	go func() {
		if err := startRedisWorker(ctx, rdb, emailSvc); err != nil {
			log.Printf("redis worker stopped with error: %v", err)
			cancel()
		}
	}()

	// Start Kafka consumer if configured
	if kafkaBrokersEnv != "" {
		brokers := strings.Split(kafkaBrokersEnv, ",")
		go func() {
			if err := startKafkaWorker(ctx, rdb, emailSvc, brokers, kafkaTopic); err != nil {
				log.Printf("kafka worker stopped with error: %v", err)
				cancel()
			}
		}()
	} else {
		log.Println("KAFKA_BROKERS not set; kafka consumer disabled")
	}

	log.Println("Worker started — Redis + Kafka (Option A). Waiting for jobs/events...")

	// wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sig:
		log.Println("shutdown signal received")
		cancel()
	case <-ctx.Done():
		log.Println("context cancelled")
	}

	// give goroutines time to stop
	time.Sleep(500 * time.Millisecond)
	log.Println("worker exiting")
}

// ----------------------- Redis worker -----------------------

func startRedisWorker(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService) error {
	log.Println("📥 Redis worker running (queue:notifications)...")
	for {
		select {
		case <-ctx.Done():
			log.Println("redis worker: context done")
			return nil
		default:
			// BRPop blocks — use 0 timeout for indefinite block
			res, err := rdb.BRPop(ctx, 0*time.Second, "queue:notifications").Result()
			if err != nil {
				// If context cancelled, return
				if err == context.Canceled || strings.Contains(err.Error(), "context canceled") {
					return nil
				}
				log.Printf("redis BRPop error: %v — retrying in 1s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			if len(res) < 2 {
				continue
			}
			raw := res[1]
			var msg tasks.NotificationPayload
			if err := json.Unmarshal([]byte(raw), &msg); err != nil {
				log.Printf("invalid job JSON from redis: %v", err)
				continue
			}

			// use idempotency and send
			if err := processAndSend(ctx, rdb, emailSvc, []byte(raw), msg.Email, msg.Subject, msg.Body); err != nil {
				log.Printf("failed sending queued email to %s: %v", msg.Email, err)
			}
		}
	}
}

// ----------------------- Kafka worker -----------------------

func startKafkaWorker(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers provided")
	}
	log.Printf("📡 Kafka worker starting; brokers=%v topic=%s", brokers, topic)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// adjust version to your cluster; using 3.4 above
	config.Version = sarama.V3_4_0_0

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	// close consumer on function exit
	// we do not defer Close here because we run goroutines per partition and must close explicitly when ctx done
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("kafka consumer close error: %v", err)
		}
	}()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}

	// start a partition goroutine
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)
		if err != nil {
			log.Printf("failed to start partition consumer %d: %v", p, err)
			continue
		}

		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-pc.Messages():
					if !ok {
						return
					}
					log.Printf("📥 Kafka Event → %s", string(msg.Value))

					// parse event
					var ke kafkaEvent
					if err := json.Unmarshal(msg.Value, &ke); err != nil {
						log.Printf("failed to unmarshal kafka event: %v", err)
						continue
					}

					// Determine email payload
					if ke.Email == "" && ke.Subject == "" && ke.Body == "" {
						// maybe typed event
						handleTypedKafkaEvent(ctx, rdb, emailSvc, ke)
					} else {
						// direct notification-style event
						if err := processAndSend(ctx, rdb, emailSvc, msg.Value, ke.Email, ke.Subject, ke.Body); err != nil {
							log.Printf("failed sending kafka notification to %s: %v", ke.Email, err)
						}
					}
				case err := <-pc.Errors():
					if err != nil {
						log.Printf("partition consumer error: %v", err)
					}
				}
			}
		}(pc)
	}

	// block until ctx cancelled
	<-ctx.Done()
	return nil
}

// handleTypedKafkaEvent converts typed events into immediate emails (Option A)
func handleTypedKafkaEvent(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService, ke kafkaEvent) {
	switch ke.Type {
	case "UserRegistered":
		sub := "Welcome!"
		body := fmt.Sprintf("Hello %s — welcome to our platform!", ke.NameOrEmail())
		rawBytes, _ := json.Marshal(map[string]string{"email": ke.Email, "subject": sub, "body": body})
		if err := processAndSend(ctx, rdb, emailSvc, rawBytes, ke.Email, sub, body); err != nil {
			log.Printf("failed sending UserRegistered email to %s: %v", ke.Email, err)
		}
	case "PasswordResetRequested":
		sub := "Password reset code"
		// check ke.Code or ke.Otp
		code := ke.Code
		if code == "" {
			code = ke.Otp
		}
		body := fmt.Sprintf("Your password reset code is: %s", code)
		rawBytes, _ := json.Marshal(map[string]string{"email": ke.Email, "subject": sub, "body": body})
		if err := processAndSend(ctx, rdb, emailSvc, rawBytes, ke.Email, sub, body); err != nil {
			log.Printf("failed sending PasswordResetRequested email to %s: %v", ke.Email, err)
		}
	case "OTPRequested":
		sub := "Your OTP code"
		body := fmt.Sprintf("Your OTP code is: %s", ke.Otp)
		rawBytes, _ := json.Marshal(map[string]string{"email": ke.Email, "subject": sub, "body": body})
		if err := processAndSend(ctx, rdb, emailSvc, rawBytes, ke.Email, sub, body); err != nil {
			log.Printf("failed sending OTPRequested email to %s: %v", ke.Email, err)
		}
	default:
		log.Printf("kafka event type not handled: %s", ke.Type)
	}
}

// helper on kafkaEvent: prefer Name, fallback to Email in greeting
func (ke *kafkaEvent) NameOrEmail() string {
	if ke.Name != "" {
		return ke.Name
	}
	return ke.Email
}

// ----------------------- send + idempotency -----------------------

func processAndSend(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService, rawPayload []byte, to, subject, body string) error {
	// compute fingerprint
	h := sha256.Sum256(rawPayload)
	fp := hex.EncodeToString(h[:])
	key := "notif:msg:" + fp

	// Try to mark as processed (SetNX). If already set, skip.
	ok, err := rdb.SetNX(context.Background(), key, "1", 24*time.Hour).Result()
	if err != nil {
		// Redis error — attempt to continue sending (but risk duplicates)
		log.Printf("redis SetNX error (idempotency): %v — proceeding to send", err)
	} else if !ok {
		// already processed
		log.Printf("skipping already-processed message (key=%s)", key)
		return nil
	}

	// send email with timeout
	sendCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if err := emailSvc.SendEmail(sendCtx, to, subject, body); err != nil {
		// On failure, remove idempotency key so message can be retried later
		if _, derr := rdb.Del(context.Background(), key).Result(); derr != nil {
			log.Printf("failed to delete idempotency key after send error: %v", derr)
		}
		return err
	}

	log.Printf("email sent to %s (subject=%s)", to, subject)
	return nil
}

// ----------------------- utils -----------------------

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
