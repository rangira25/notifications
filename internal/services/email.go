package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net/smtp"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type EmailService struct {
	host string
	port int
	user string
	pass string
}

func NewEmailService(host string, portStr string, user, pass string) *EmailService {
	p, _ := strconv.Atoi(portStr)
	return &EmailService{
		host: host,
		port: p,
		user: user,
		pass: pass,
	}
}

// SendEmail sends a raw email
func (s *EmailService) SendEmail(ctx context.Context, to, subject, body string) error {
	if s.host == "" || s.port == 0 {
		log.Println("EmailService NOOP mode, skipping email send.")
		return nil
	}

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	msg := []byte(fmt.Sprintf("To: %s\r\nSubject: %s\r\n\r\n%s", to, subject, body))
	auth := smtp.PlainAuth("", s.user, s.pass, s.host)

	done := make(chan error, 1)
	go func() { done <- smtp.SendMail(addr, auth, s.user, []string{to}, msg) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			log.Printf("smtp send error: %v\n", err)
			return err
		}
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("smtp send timeout")
	}
}

// SendEmailWithIdempotency ensures each payload is only sent once
func SendEmailWithIdempotency(ctx context.Context, rdb *redis.Client, svc *EmailService, to, subject, body string, payload []byte) error {
	hash := sha256.Sum256(payload)
	key := "notif:msg:" + hex.EncodeToString(hash[:])

	ok, err := rdb.SetNX(ctx, key, "1", 24*time.Hour).Result()
	if err != nil {
		log.Printf("redis SetNX error (idempotency): %v — proceeding to send", err)
	} else if !ok {
		log.Printf("skipping already-processed message (key=%s)", key)
		return nil
	}

	if err := svc.SendEmail(ctx, to, subject, body); err != nil {
		rdb.Del(ctx, key) // rollback idempotency key
		return err
	}

	log.Printf("email sent to %s (subject=%s)", to, subject)
	return nil
}

// For local dev/testing
func NewNoopEmailService() *EmailService {
	return &EmailService{}
}
