package redislock

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"

	mContext "github.com/rentiansheng/incenses/src/context"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

var cache *redis.Client

func SetClient(c *redis.Client) {
	cache = c
}

// Lock 获取执行锁, lockedExpireMinute 单位是Nanosecond， 最后回转换为 Millisecond
func Lock(ctx context.Context, key string, lockedExpireMinute time.Duration) (bool, error) {
	rid := mContext.CtxLogID(ctx)

	// 是否可以执行任务
	ok, err := cache.SetNX(ctx, key, []byte(rid), lockedExpireMinute/time.Millisecond).Result()
	if err != nil {
		return false, err
	}

	return ok, nil

}

// Unlock 释放锁
func Unlock(ctx context.Context, key string) error {
	rid := mContext.CtxLogID(ctx)

	val, err := cache.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	// 判断是否是否当前任务锁的
	if string(val) == rid {
		if err := cache.Del(ctx, key).Err(); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unauthorized operation")
	}

	return nil
}
