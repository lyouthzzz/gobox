package gormbox

import (
	"testing"
)

func TestLoggerPrint(t *testing.T) {
	globalLogger.Info("message")

	globalLogger.Error("message")
}
