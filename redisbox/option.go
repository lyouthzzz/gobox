package redisbox

import "go.uber.org/zap"

type Option func(*options)

type options struct {
	logger *zap.Logger
}

func OptionLogger(logger *zap.Logger) Option {
	return func(o *options) { o.logger = logger }
}
