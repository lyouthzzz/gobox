package gormbox

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfig_Build(t *testing.T) {
	db := DefaultConfig().WithDSN("root:root@tcp(127.0.0.1:3306)/web_layout?charset=utf8mb4&parseTime=True&loc=Local").BuildMust()

	var m map[string]interface{}

	err := db.WithContext(WithOperation(context.Background(), "demo")).Table("user").Find(&m).Error
	require.NoError(t, err)

	fmt.Println(m)
}
