package redisbox

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfig_Build(t *testing.T) {
	cfg := DefaultConfig().WithAddr("localhost:6379").WithDB(1).WithPassword("root")

	require.Equal(t, cfg.Addr, []string{"localhost:6379"})
	require.Equal(t, cfg.Db, int32(1))
	require.Equal(t, cfg.Password, "root")

	rdb := cfg.BuildMust()
	require.NoError(t, rdb.Set(WithOperation(context.Background(), "setOne"), "k", "v", 0).Err())
}
