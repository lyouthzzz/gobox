package gormbox

import (
	"context"
	"errors"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type operation struct{}

func WithOperation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, operation{}, id)
}

func OperationFrom(ctx context.Context) string {
	if sqlId, ok := ctx.Value(operation{}).(string); ok {
		return sqlId
	}
	return ""
}

func IsRecordDuplicate(err error) bool {
	var mysqlErr = &mysql.MySQLError{}
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}
