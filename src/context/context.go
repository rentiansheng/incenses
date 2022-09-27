package context

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/rentiansheng/incenses/src/context/log"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/21
    @desc:

***************************/

const (
	rootIDKey      = "__root_id"
	logIDKeyPrefix = "metric:"
)

var (
	defaultLog log.LogImpl
)

type CancelFunc = context.CancelFunc

type Context interface {
	context.Context
	SubCtx(prefix string) Context
	Cancel() context.CancelFunc
	IsDone() bool
	WithTimeout(timeout time.Duration)
	WithValue(key string, val interface{})

	Log() log.LogImpl
}

func TODO() Context {
	return NewContexts(context.TODO())
}

func SetLog(log log.LogImpl) {
	defaultLog = log
}

func Background() Context {
	return NewContexts(context.Background())
}

func NewContexts(ctx context.Context) Context {
	if rCtx, ok := ctx.(Context); ok {
		return rCtx
	}
	if defaultLog == nil {
		defaultLog = log.DefaultLog()
	}

	var rootID string
	ctx, rootID = setLogID(ctx, rootIDKey)
	return &contexts{
		ctx:    ctx,
		log:    defaultLog,
		rootID: rootID,
	}
}

type contexts struct {
	ctx    context.Context
	rootID string
	log    log.LogImpl
}

func (c contexts) SubCtx(prefix string) Context {
	return c.setRootLogIDPrefix(c.ctx, prefix)
}

func (c *contexts) Cancel() context.CancelFunc {
	ctx, cancel := context.WithCancel(c.ctx)
	c.ctx = ctx
	return cancel
}

func (c *contexts) WithTimeout(timeout time.Duration) {
	c.ctx, _ = context.WithTimeout(c.ctx, timeout)
	return
}

func (c *contexts) WithValue(key string, val interface{}) {
	c.ctx = context.WithValue(c.ctx, key, val)
	return
}

func (c *contexts) IsDone() bool {
	select {
	case <-c.Done():
		return true
	default:
		return false
	}
}

func (c contexts) setRootLogIDPrefix(ctx context.Context, prefix string) Context {
	if c.ctx == nil {
		c.ctx = context.TODO()
	}
	key := rootIDKey
	requestID, ok := c.ctx.Value(key).(string)
	if !ok || requestID == "" {
		requestID = logIDKeyPrefix + uuid.NewString()
	}
	requestID += ":" + prefix

	ctx = context.WithValue(c.ctx, key, requestID)

	newC := c.clone()
	newC.ctx = ctx
	newC.rootID = requestID
	return newC
}

func (c contexts) Log() log.LogImpl {
	return c.log
}

func (c contexts) clone() *contexts {
	newC := &contexts{
		ctx: c.ctx,
		log: c.log,
	}
	return newC
}

func setLogID(ctx context.Context, key string) (context.Context, string) {
	if ctx == nil {
		ctx = context.TODO()
	}

	requestID, ok := ctx.Value(key).(string)
	if !ok || requestID == "" {
		requestID = logIDKeyPrefix + uuid.NewString()
		ctx = context.WithValue(ctx, key, requestID)
	}

	return ctx, requestID
}

func CtxLogID(ctx context.Context) string {
	if ctx == nil {
		ctx = context.TODO()
	}

	requestID, ok := ctx.Value(rootIDKey).(string)
	if ok {
		return requestID
	}

	return requestID
}
