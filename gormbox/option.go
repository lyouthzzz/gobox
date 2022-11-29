package gormbox

import "go.uber.org/zap"

type Option func(*options)

type options struct {
	logger *zap.Logger
}
