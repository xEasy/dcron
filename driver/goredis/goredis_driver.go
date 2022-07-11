package goredis

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/libi/dcron/driver"
)

// GlobalKeyPrefix is global redis key preifx
const GlobalKeyPrefix = "distributed-cron:"

type RedisDriver struct {
	ctx     context.Context
	rdb     *redis.Client
	timeout time.Duration
}

// make EDITOR check RedisDriver impl Driver
var _ driver.Driver = new(RedisDriver)

func NewDriver(redisOpt *redis.Options) (*RedisDriver, error) {
	return &RedisDriver{
		ctx: context.Background(),
		rdb: redis.NewClient(redisOpt),
	}, nil
}

// Ping is check dirver is valid
func (r *RedisDriver) Ping() error {
	_, err := r.rdb.Ping(r.ctx).Result()
	return err
}

func (r *RedisDriver) getKeyPre(serviceName string) string {
	return strings.Join([]string{GlobalKeyPrefix, serviceName, ":"}, "")
}

func (r *RedisDriver) SetHeartBeat(nodeID string) {
	go r.heartBear(nodeID)
}

func (r *RedisDriver) heartBear(nodeID string) {
	key := nodeID
	tickers := time.NewTicker(r.timeout / 2)

	for range tickers.C {
		keyExist, err := r.rdb.Expire(r.ctx, key, r.timeout).Result()
		if err != nil {
			log.Printf("redis expire error %+v", err)
			continue
		}
		if !keyExist {
			if err := r.registerServiceNode(nodeID); err != nil {
				log.Printf("register service node error %+v", err)
			}
		}
	}
}

func (r *RedisDriver) registerServiceNode(nodeID string) error {
	_, err := r.rdb.SetEX(r.ctx, nodeID, nodeID, r.timeout).Result()
	return err
}

func (r *RedisDriver) SetTimeout(timeout time.Duration) {
	r.timeout = timeout
}

func (r *RedisDriver) GetServiceNodeList(serviceName string) ([]string, error) {
	mathStr := strings.Join([]string{r.getKeyPre(serviceName), "*"}, "")
	cursor := uint64(0)
	result := make([]string, 0)
	for {
		vals, cursorResp, err := r.rdb.Scan(r.ctx, cursor, mathStr, 10).Result()
		if err != nil {
			return nil, err
		}
		cursor = cursorResp
		result = append(result, vals...)
		if cursor == 0 {
			break
		}
	}
	return result, nil
}

func (r *RedisDriver) RegisterServiceNode(serviceName string) (nodeID string, err error) {
	nodeID = r.getKeyPre(serviceName) + uuid.New().String()
	if err := r.registerServiceNode(nodeID); err != nil {
		return "", err
	}
	return nodeID, nil
}
