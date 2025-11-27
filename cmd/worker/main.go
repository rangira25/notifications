package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/notification/internal/worker"
)

func main() {
	_ = godotenv.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})

	emailSvc := services.NewEmailService(
		os.Getenv("SMTP_HOST"),
		os.Getenv("SMTP_PORT"),
		os.Getenv("SMTP_USER"),
		os.Getenv("SMTP_PASS"),
	)

	kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")

	go worker.StartRedisWorker(ctx, rdb, emailSvc)
	go worker.StartKafkaWorker(ctx, rdb, emailSvc, kafkaBrokers, kafkaTopic)

	log.Println("🚀 Notification Worker Started (Redis + Kafka)")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("shutdown signal received, exiting...")
	cancel()
	time.Sleep(500 * time.Millisecond)
}
