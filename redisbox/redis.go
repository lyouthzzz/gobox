package redisbox

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

type operationKey struct{}

func WithOperation(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, operationKey{}, operation)
}

func OperationFrom(ctx context.Context) string {
	if operation, ok := ctx.Value(operationKey{}).(string); ok {
		return operation
	}
	return ""
}

func IsNil(err error) bool {
	return errors.Is(err, redis.Nil)
}
