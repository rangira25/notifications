package handlers

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/rangira25/notification/internal/services"
)

type KafkaEvent struct {
	Type    string `json:"type,omitempty"`
	Email   string `json:"email,omitempty"`
	Name    string `json:"name,omitempty"`
	Subject string `json:"subject,omitempty"`
	Body    string `json:"body,omitempty"`
	Otp     string `json:"otp,omitempty"`
	Code    string `json:"code,omitempty"`
}

func (ke *KafkaEvent) NameOrEmail() string {
	if ke.Name != "" {
		return ke.Name
	}
	return ke.Email
}

// HandleTypedEvent processes typed events and sends email
func HandleTypedEvent(ctx context.Context, rdb *redis.Client, emailSvc *services.EmailService, ev KafkaEvent) {
	switch ev.Type {
	case "UserRegistered":
		sub := "Welcome!"
		body := fmt.Sprintf("Hello %s — welcome to our platform!", ev.NameOrEmail())
		_ = services.SendEmailWithIdempotency(ctx, rdb, emailSvc, ev.Email, sub, body, []byte(body))
	case "PasswordResetRequested":
		code := ev.Code
		if code == "" {
			code = ev.Otp
		}
		sub := "Password reset code"
		body := fmt.Sprintf("Your password reset code is: %s", code)
		_ = services.SendEmailWithIdempotency(ctx, rdb, emailSvc, ev.Email, sub, body, []byte(body))
	case "OTPRequested":
		sub := "Your OTP code"
		body := fmt.Sprintf("Your OTP code is: %s", ev.Otp)
		_ = services.SendEmailWithIdempotency(ctx, rdb, emailSvc, ev.Email, sub, body, []byte(body))
	default:
		log.Printf("unhandled Kafka event type: %s", ev.Type)
	}
}
