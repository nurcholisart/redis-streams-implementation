package client

import (
	"context"

	"github.com/go-redis/redis/v8"
)

var (
	ctx = context.Background()
	rdb *redis.Client
)

func NewRedisClient() (*redis.Client, error) {
	opt, err := redis.ParseURL("redis://127.0.0.1:6379/0")
	if err != nil {
		panic(err)
	}

	rdb = redis.NewClient(opt)
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	return rdb, nil
}
