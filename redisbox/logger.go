package redisbox

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var globalLogger *zap.Logger

func init() {
	cfg := zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		TimeKey:        "log_time",
		StacktraceKey:  "exception_stack",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	globalLogger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(cfg), os.Stdout, zap.InfoLevel), zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
}

func SetLogger(logger *zap.Logger) {
	globalLogger = logger
}
