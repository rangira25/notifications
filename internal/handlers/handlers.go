package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/rangira25/notification/internal/cache"
	"github.com/rangira25/notification/internal/kafka"
	"github.com/rangira25/notification/internal/models"
	"github.com/rangira25/notification/internal/tasks"
)

type Handler struct {
	DB       *gorm.DB
	Redis    *redis.Client
	Asynq    *asynq.Client
	Logger   *zap.Logger
	AppCache *cache.RedisCache
	Producer *kafka.Producer // optional, can be nil if Kafka disabled
}

func NewHandler(db *gorm.DB, r *redis.Client, a *asynq.Client, l *zap.Logger, ac *cache.RedisCache, p *kafka.Producer) *Handler {
	return &Handler{DB: db, Redis: r, Asynq: a, Logger: l, AppCache: ac, Producer: p}
}

type CreateUserReq struct {
	FullName string `json:"full_name" binding:"required"`
	Email    string `json:"email" binding:"required,email"`
	Phone    string `json:"phone"`
	ReqID    string `json:"req_id" binding:"required"`
}

type UserResponse struct {
	ID       string    `json:"id"`
	FullName string `json:"full_name"`
	Email    string `json:"email"`
}

// ------------------------ Core Logic ------------------------

func (h *Handler) CreateUserLogic(ctx context.Context, req CreateUserReq) (*UserResponse, error) {
	// idempotency check
	idemKey := "idem:req:" + req.ReqID
	ok, err := h.Redis.SetNX(ctx, idemKey, "1", 24*time.Hour).Result()
	if err != nil {
		h.Logger.Error("redis setnx error", zap.Error(err))
		return nil, errors.New("internal")
	}
	if !ok {
		return nil, errors.New("duplicate request")
	}

	// create user in DB
	user := models.User{
		FullName: req.FullName,
		Email:    req.Email,
	}

	if err := h.DB.Create(&user).Error; err != nil {
		h.Redis.Del(ctx, idemKey)
		h.Logger.Error("db create failed", zap.Error(err))
		return nil, errors.New("db error")
	}

	// enqueue welcome email
	payload := &tasks.WelcomeEmailPayload{
		UserID: user.ID,
		Email:  user.Email,
		ReqID:  req.ReqID,
	}
	if _, err := tasks.EnqueueWelcomeEmail(h.Asynq, payload); err != nil {
		h.Redis.Del(ctx, idemKey)
		h.Logger.Error("enqueue failed", zap.Error(err))
		return nil, errors.New("enqueue failed")
	}

	return &UserResponse{
		ID:       user.ID,
		FullName: user.FullName,
		Email:    user.Email,
	}, nil
}

// ------------------------ HTTP Handlers ------------------------

// CreateUser wraps core logic for regular endpoint
func (h *Handler) CreateUser(c *gin.Context) {
	var req CreateUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Warn("bad request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userResp, err := h.CreateUserLogic(c.Request.Context(), req)
	if err != nil {
		status := http.StatusInternalServerError
		if err.Error() == "duplicate request" {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, userResp)
}

// CreateUserCached wraps logic and caches the response
func (h *Handler) CreateUserCached(c *gin.Context) {
	var req CreateUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		h.Logger.Warn("bad request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()
	cacheKey := "user:create:" + req.Email

	// check cache
	if cached, err := h.AppCache.Get(ctx, cacheKey); err == nil && cached != "" {
		var cachedObj map[string]interface{}
		_ = json.Unmarshal([]byte(cached), &cachedObj)
		c.JSON(http.StatusOK, gin.H{"status": "cached", "data": cachedObj})
		return
	}

	// call logic
	userResp, err := h.CreateUserLogic(ctx, req)
	if err != nil {
		status := http.StatusInternalServerError
		if err.Error() == "duplicate request" {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}

	// cache result
	data, err := json.Marshal(userResp)
	if err == nil {
		_ = h.AppCache.Set(ctx, cacheKey, string(data))
	}

	c.JSON(http.StatusCreated, userResp)
}

// Health endpoint
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// PublishNotification publishes a notification to Kafka
func (h *Handler) PublishNotification(c *gin.Context) {
	if h.Producer == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kafka producer not configured"})
		return
	}

	var payload struct {
		Email   string `json:"email" binding:"required"`
		Subject string `json:"subject" binding:"required"`
		Body    string `json:"body" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	topic := "notifications"
	msg := kafka.NotificationMessage{
		Email:   payload.Email,
		Subject: payload.Subject,
		Body:    payload.Body,
	}

	if err := h.Producer.SendJSON(topic, msg); err != nil {
		h.Logger.Error("failed to send kafka message", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue notification"})
		return
	}

	// cache last notification
	go func() {
		_ = h.AppCache.Set(context.Background(), "user:last_notification:"+payload.Email, payload.Subject)
	}()

	c.JSON(http.StatusOK, gin.H{"status": "queued"})
}
