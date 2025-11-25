package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/rangira25/notification/internal/cache"
	"github.com/rangira25/notification/internal/config"
	"github.com/rangira25/notification/internal/handlers"
	"github.com/rangira25/notification/internal/kafka"
	"github.com/rangira25/notification/internal/logging"
	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/notification/internal/storage"
)

func main() {
	// load .env
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found, using environment variables")
	}

	// config & logger
	cfg := config.Load()
	logger := logging.NewLogger(cfg.Env == "development")
	defer logger.Sync()

	// infra
	db := storage.NewGorm(cfg.DatabaseURL)
	redisClient := storage.NewRedis(cfg.RedisAddr, cfg.RedisPassword)
	appCache := cache.NewRedisCache(redisClient, 5*time.Minute)
	asynqClient := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	services.NewEmailService(cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUser, cfg.SMTPPass)

	// Kafka producer (optional)
	var producer *kafka.Producer
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		brokerList := strings.Split(brokers, ",")
		p, err := kafka.NewProducer(brokerList)
		if err != nil {
			logger.Error("failed to create kafka producer", zap.Error(err))
		} else {
			producer = p
			logger.Info("kafka producer initialized")
		}
	} else {
		logger.Info("no kafka brokers configured; kafka producer disabled")
	}

	// handler
	h := handlers.NewHandler(db, redisClient, asynqClient, logger, appCache, producer)

	// Gin setup
	r := gin.New()
	r.Use(gin.Recovery())

	// Logging middleware
	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)))
	})

	// Prometheus metrics
	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = reg
	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))

	// routes
	r.GET("/health", h.Health)
	r.POST("/api/v1/users", h.CreateUser)
	r.POST("/api/v1/users_cached", h.CreateUserCached)
	r.POST("/api/v1/notifications", h.PublishNotification)

	// server setup
	srv := &http.Server{
		Addr:    cfg.APIAddr,
		Handler: r,
	}

	// graceful start
	go func() {
		logger.Info("starting api", zap.String("addr", cfg.APIAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("listen failed", zap.Error(err))
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	logger.Info("shutting down api")
	_ = srv.Shutdown(context.Background())
	asynqClient.Close()
	logger.Info("api stopped")
}
