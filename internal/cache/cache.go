package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache is a tiny wrapper for app-level caching.
// It wraps an existing *redis.Client (so it matches how your storage.NewRedis returns the client).
type RedisCache struct {
	Client *redis.Client
	TTL    time.Duration
}

// NewRedisCache returns a new RedisCache that uses the provided redis client and default TTL.
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{
		Client: client,
		TTL:    ttl,
	}
}

// Get returns the string value for a key. If key doesn't exist, an error is returned (redis.Nil).
func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	return c.Client.Get(ctx, key).Result()
}

// Set stores a value for the default cache TTL.
func (c *RedisCache) Set(ctx context.Context, key string, value string) error {
	return c.Client.Set(ctx, key, value, c.TTL).Err()
}

// SetNX sets a key only if it does not exist and returns true if the key was set.
func (c *RedisCache) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	return c.Client.SetNX(ctx, key, value, ttl).Result()
}
