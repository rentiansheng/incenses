package log

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/10/5
    @desc:

***************************/
var (
	defaultLog LogImpl
)

type log struct {
	fields []FieldImpl
	log    *zap.Logger
}

func DefaultLog() LogImpl {
	if defaultLog != nil {
		return defaultLog
	}

	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(os.Stdout), zapcore.DebugLevel)
	logger := zap.New(core, zap.WithCaller(true), zap.AddCallerSkip(1))
	defaultLog = log{
		log: logger,
	}
	return defaultLog
}

func NewLog(core zapcore.Core) LogImpl {
	l := zap.New(core, zap.WithCaller(true), zap.AddCallerSkip(1))

	defaultLog = log{
		log: l,
	}
	return defaultLog
}

func (l log) Field(key string, val interface{}) LogImpl {
	l.fields = append(l.fields, Field(key, val))
	return l
}

func (l log) Fields(fields ...FieldImpl) LogImpl {
	l.fields = append(l.fields, fields...)
	return l
}

func (l log) toZAPFields() []zap.Field {
	fields := make([]zap.Field, len(l.fields))
	for idx, field := range l.fields {
		fields[idx] = zap.Any(field.Field())
	}
	return fields
}

func (l log) Errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.log.Error(msg, l.toZAPFields()...)
}

func (l log) Infof(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.log.Info(msg, l.toZAPFields()...)
}

func (l log) Debugf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.log.Debug(msg, l.toZAPFields()...)
}

func (l log) Error(msg string) {
	l.log.Error(msg, l.toZAPFields()...)
}

func (l log) Info(msg string) {
	l.log.Info(msg, l.toZAPFields()...)
}

func (l log) Debug(msg string) {
	l.log.Debug(msg, l.toZAPFields()...)
}

func (l log) Sync() {
	l.log.Sync()
}

func (l log) Panicf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)

	l.log.Panic(msg, l.toZAPFields()...)
}

var _ LogImpl = (*log)(nil)
