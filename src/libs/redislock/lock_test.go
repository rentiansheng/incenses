package redislock

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v9"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/27
    @desc:

***************************/

func initClient() (*miniredis.Miniredis, error) {
	m, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	cache := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    m.Addr(),
		DB:      0,
	})
	if err := cache.Ping(context.TODO()).Err(); err != nil {
		return nil, err
	}
	SetClient(cache)
	return m, nil
}

func TestLock(t *testing.T) {

	miniredisDB, err := initClient()
	require.NoError(t, err, "test lock  init error")

	key := "metric:test:key"
	keyExpire := time.Minute * 10

	tests := []struct {
		hooks   []func() error
		name    string
		want    bool
		wantErr bool
	}{
		{
			nil,
			"lock true",
			true,
			false,
		},
		{
			nil,
			"lock false",
			false,
			false,
		},
		{
			[]func() error{func() error { miniredisDB.FastForward(keyExpire); return nil }},
			"expire lock true ",
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, hook := range tt.hooks {
				require.NoError(t, hook(), "hook error")
			}

			got, err := Lock(context.TODO(), key, keyExpire)
			if (err != nil) != tt.wantErr {
				t.Errorf("Lock() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Lock() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnlock(t *testing.T) {

	_, err := initClient()
	require.NoError(t, err, "test lock  init error")

	key := "metric:test:key"
	keyExpire := time.Minute * 10

	ctx := context.TODO()
	err = Unlock(ctx, key)
	require.Error(t, err, redis.Nil, "unlock error")

	locked, err := Lock(context.TODO(), key, keyExpire)
	require.NoError(t, err, "lock error")
	require.Equal(t, true, locked, "lock error ")

	err = Unlock(ctx, key)
	require.NoError(t, err, "unlock error")
}
