package goredis

import (
	"log"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRedisDriver_Scan(t *testing.T) {
	rd, err := NewDriver(&redis.Options{
		Addr: "localhost:6379",
		DB:   2,
	})
	if err != nil {
		log.Fatal(err)
	}

	rd.SetTimeout(time.Second * 10)

	nodeList, err := rd.GetServiceNodeList("gord")
	if err != nil {
		t.Fatal("GetServiceNodeList fail: ", err)
	}
	if len(nodeList) > 0 {
		_, err = rd.rdb.Del(rd.ctx, nodeList...).Result()
		if err != nil {
			log.Fatal("del old test keys fail, err:", err)
		}
	}

	for i := 0; i < 100; i++ {
		_, err := rd.RegisterServiceNode("gord")
		if err != nil {
			log.Fatal("RegisterServiceNode fail: ", err)
		}
	}

	nodeList, err = rd.GetServiceNodeList("gord")
	if err != nil {
		t.Fatal("GetServiceNodeList fail: ", err)
	}

	if len(nodeList) != 100 {
		t.Fatalf("GetServiceNodeList wrong expect 100, got %d", len(nodeList))
	}
}
