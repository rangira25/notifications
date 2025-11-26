package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Handler struct {
	DB     *gorm.DB
	Logger *zap.Logger
}

func NewHandler(db *gorm.DB, logger *zap.Logger) *Handler {
	return &Handler{
		DB:     db,
		Logger: logger,
	}
}

// Health endpoint
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
