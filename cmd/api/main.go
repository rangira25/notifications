package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/rangira25/notification/internal/config"
	"github.com/rangira25/notification/internal/handlers"
	"github.com/rangira25/notification/internal/logging"
	"github.com/rangira25/notification/internal/services"
	"github.com/rangira25/notification/internal/storage"
)

func main() {
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found, using environment variables")
	}

	// Config & logger
	cfg := config.Load()
	logger := logging.NewLogger(cfg.Env == "development")
	defer logger.Sync()

	// Infra
	db := storage.NewGorm(cfg.DatabaseURL)
	services.NewEmailService(cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUser, cfg.SMTPPass)

	// Handler
	h := handlers.NewHandler(db, logger)

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

	// Routes
	r.GET("/health", h.Health)

	// Server setup
	srv := &http.Server{
		Addr:    cfg.APIAddr,
		Handler: r,
	}

	// Graceful start
	go func() {
		logger.Info("starting api", zap.String("addr", cfg.APIAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("listen failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	logger.Info("shutting down api")
	_ = srv.Shutdown(context.Background())
	logger.Info("api stopped")
}
