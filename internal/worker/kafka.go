package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/notification/internal/handlers"
)

type KafkaConsumerGroupHandler struct {
	rdb      *redis.Client
	emailSvc *services.EmailService
}

func (h *KafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *KafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *KafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("📥 Kafka Event → %s", string(msg.Value))

		var ev handlers.KafkaEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("unmarshal kafka event error: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		// typed or generic
		if ev.Email == "" && ev.Subject == "" && ev.Body == "" {
			handlers.HandleTypedEvent(context.Background(), h.rdb, h.emailSvc, ev)
		} else {
			_ = services.SendEmailWithIdempotency(context.Background(), h.rdb, h.emailSvc, ev.Email, ev.Subject, ev.Body, msg.Value)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func StartKafkaWorker(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return nil
	}

	groupID := "notification-worker-group"
	log.Printf("📡 Kafka Worker (group=%s) starting; brokers=%v topic=%s", groupID, brokers, topic)

	config := sarama.NewConfig()
	config.Version = sarama.V3_4_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return err
	}

	go func() {
		for err := range group.Errors() {
			log.Println("❌ Kafka consumer group error:", err)
		}
	}()

	handler := &KafkaConsumerGroupHandler{rdb: rdb, emailSvc: emailSvc}

	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			log.Println("❌ Kafka consume error:", err)
			time.Sleep(2 * time.Second)
		}
	}
}
