package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"

    "github.com/joho/godotenv"

    "github.com/redis/go-redis/v9"
    "github.com/IBM/sarama"

    "github.com/rangira25/notification/internal/services"
    "github.com/rangira25/user_service/shared/tasks"
)

func main() {
    // Load .env
    godotenv.Load()

    // -------------------------
    // Email Service
    // -------------------------
    emailSvc := services.NewEmailService(
        os.Getenv("SMTP_HOST"),
        os.Getenv("SMTP_PORT"),
        os.Getenv("SMTP_USER"),
        os.Getenv("SMTP_PASS"),
    )

    // -------------------------
    // Redis Client
    // -------------------------
    rdb := redis.NewClient(&redis.Options{
        Addr:     os.Getenv("REDIS_ADDR"),
        Password: "",
        DB:       0,
    })

    // -------------------------
    // Kafka Config
    // -------------------------
    kafkaBrokers := []string{os.Getenv("KAFKA_BROKER")}
    kafkaTopic := os.Getenv("KAFKA_TOPIC")

    go startRedisWorker(rdb, emailSvc)
    go startKafkaWorker(kafkaBrokers, kafkaTopic)

    fmt.Println("🚀 Notification Worker Started (Redis + Kafka)")

    select {} // block forever
}

//
// ──────────────────────────────────────────────────────────────
//   REDIS WORKER – EMAIL SENDER
// ──────────────────────────────────────────────────────────────
//

func startRedisWorker(rdb *redis.Client, emailSvc *services.EmailService) {
    ctx := context.Background()

    fmt.Println("📥 Redis Worker running…")

    for {
        res, err := rdb.BLPop(ctx, 0, "queue:notifications").Result()
        if err != nil {
            fmt.Println("❌ Redis error:", err)
            continue
        }

        var payload tasks.NotificationPayload
        if err := json.Unmarshal([]byte(res[1]), &payload); err != nil {
            fmt.Println("❌ Invalid JSON payload:", err)
            continue
        }

        fmt.Printf("📨 Sending email → %s\n", payload.Email)

        err = emailSvc.SendEmail(ctx, payload.Email, payload.Subject, payload.Body)
        if err != nil {
            fmt.Println("❌ Email error:", err)
        } else {
            fmt.Println("✅ Email sent successfully!")
        }
    }
}

//
// ──────────────────────────────────────────────────────────────
//   KAFKA WORKER – EVENT LISTENER
// ──────────────────────────────────────────────────────────────
//

func startKafkaWorker(brokers []string, topic string) {
    fmt.Println("📡 Kafka Worker running…")

    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true
    config.Version = sarama.V3_4_0_0

    consumer, err := sarama.NewConsumer(brokers, config)
    if err != nil {
        log.Fatalf("❌ Kafka consumer error: %v", err)
    }

    partitions, err := consumer.Partitions(topic)
    if err != nil {
        log.Fatalf("❌ Kafka partitions error: %v", err)
    }

    for _, p := range partitions {
        go func(partition int32) {
            pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
            if err != nil {
                log.Printf("❌ Partition error %v: %v\n", partition, err)
                return
            }

            fmt.Println("📡 Kafka listening on partition:", partition)

            for msg := range pc.Messages() {
                fmt.Printf("📥 Kafka Event → %s\n", string(msg.Value))
                // Later: process events, analytics, etc.
            }
        }(p)
    }
}
