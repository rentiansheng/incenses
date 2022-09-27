package define

import (
	"context"
	"time"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type Lock interface {
	Lock(ctx context.Context, key string, lockedExpireMinute time.Duration) (bool, error)
	Unlock(ctx context.Context, key string) error
}

const (
	LockKeyPrefix = "metric:task:lock:"
)
