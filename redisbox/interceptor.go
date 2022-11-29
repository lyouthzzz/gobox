package redisbox

import (
	"context"
	"github.com/go-redis/redis/extra/rediscmd/v8"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.6.1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

var (
	_ redis.Hook = (*hookLogger)(nil)
	_ redis.Hook = (*hookTrace)(nil)
	_ redis.Hook = (*hookMetric)(nil)
)

func newHookTrace(dsn *DSN) redis.Hook {
	return &hookTrace{
		dsn:    dsn,
		tracer: otel.Tracer("redis"),
	}
}

func newHookLogger(dsn *DSN, logger *zap.Logger) redis.Hook {
	return &hookLogger{dsn: dsn, logger: logger}
}
func newHookMetric(dsn *DSN) redis.Hook {
	requestsTotals := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis",
		Subsystem: "requests",
		Name:      "totals",
		Help:      "The total number of db operation",
	}, []string{"redis_instance", "db", "operation"})

	requestLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "redis",
		Subsystem:   "requests",
		Name:        "latency_seconds",
		Help:        "The second latency of db operation",
		ConstLabels: nil,
	}, []string{"redis_instance", "db", "operation"})
	prometheus.MustRegister(requestsTotals, requestLatency)
	return &hookMetric{dsn: dsn, requestsTotals: requestsTotals, requestsLatency: requestLatency}
}

type DSN struct {
	Addr []string
	DB   int
}

type Interceptor func(*DSN) redis.Hook

type hookTrace struct {
	dsn    *DSN
	tracer trace.Tracer
}

func (hook *hookTrace) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return ctx, nil
	}
	ctx, span := hook.tracer.Start(ctx, operation)
	span.SetAttributes(semconv.DBSystemRedis)
	span.SetAttributes(semconv.DBConnectionStringKey.StringSlice(hook.dsn.Addr))
	span.SetAttributes(attribute.Int("db.name", hook.dsn.DB))
	span.SetAttributes(semconv.DBStatementKey.String(rediscmd.CmdString(cmd)))
	span.SetAttributes(semconv.DBOperationKey.String(operation))
	return ctx, nil
}

func (hook *hookTrace) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if IsActualError(cmd.Err()) {
		span.RecordError(cmd.Err())
	}
	span.End()
	return nil
}

func (hook *hookTrace) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return ctx, nil
	}
	_, cmdAction := rediscmd.CmdsString(cmds)
	ctx, span := hook.tracer.Start(ctx, operation)
	span.SetAttributes(semconv.DBSystemRedis)
	span.SetAttributes(semconv.DBConnectionStringKey.StringSlice(hook.dsn.Addr))
	span.SetAttributes(semconv.DBNameKey.String(strconv.FormatInt(int64(hook.dsn.DB), 10)))
	span.SetAttributes(semconv.DBStatementKey.String(cmdAction))
	span.SetAttributes(semconv.DBOperationKey.String(operation))
	return ctx, nil
}

func (hook *hookTrace) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if IsActualError(cmds[0].Err()) {
		span.RecordError(cmds[0].Err())
	}
	span.End()
	return nil
}

type hookLogger struct {
	dsn    *DSN
	logger *zap.Logger
}

type loggerTimeKey struct{}

func (hook *hookLogger) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return ctx, nil
	}
	return context.WithValue(ctx, loggerTimeKey{}, time.Now()), nil
}

func (hook *hookLogger) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return nil
	}
	st, ok := ctx.Value(loggerTimeKey{}).(time.Time)
	if !ok {
		return nil
	}
	var (
		latency = time.Since(st)
		message strings.Builder
	)
	message.WriteString("db.operation=" + operation)
	message.WriteString("\t")
	message.WriteString("db.connection_string=" + strings.Join(hook.dsn.Addr, ","))
	message.WriteString("\t")
	message.WriteString("db.name=" + strconv.FormatInt(int64(hook.dsn.DB), 10))
	message.WriteString("\t")
	message.WriteString("db.statement=" + rediscmd.CmdString(cmd))
	message.WriteString("\t")
	message.WriteString("latency=" + latency.String())
	message.WriteString("\t")

	traceId := trace.SpanContextFromContext(ctx).TraceID().String()
	spanId := trace.SpanContextFromContext(ctx).SpanID().String()

	if IsActualError(cmd.Err()) {
		hook.logger.Error(message.String(),
			zap.String("trace_id", traceId),
			zap.String("span_id", spanId),
			zap.String("exception_msg", cmd.Err().Error()),
			zap.String("exception_type", "redis"),
		)
	} else {
		hook.logger.Info(message.String(),
			zap.String("trace_id", traceId),
			zap.String("span_id", spanId),
		)
	}
	return nil
}

func (hook *hookLogger) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return ctx, nil
	}
	return context.WithValue(ctx, loggerTimeKey{}, time.Now()), nil
}

func (hook *hookLogger) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return nil
	}
	st, ok := ctx.Value(loggerTimeKey{}).(time.Time)
	if !ok {
		return nil
	}
	_, cmdAction := rediscmd.CmdsString(cmds)

	var message strings.Builder
	message.WriteString("db.operation=" + operation)
	message.WriteString("\t")
	message.WriteString("db.connection_string=" + strings.Join(hook.dsn.Addr, ","))
	message.WriteString("\t")
	message.WriteString("db.name=" + strconv.FormatInt(int64(hook.dsn.DB), 10))
	message.WriteString("\t")
	message.WriteString("db.statement=" + cmdAction)
	message.WriteString("\t")
	message.WriteString("latency=" + time.Since(st).String())
	message.WriteString("\t")

	traceId := trace.SpanContextFromContext(ctx).TraceID().String()
	spanId := trace.SpanContextFromContext(ctx).SpanID().String()

	if IsActualError(cmds[0].Err()) {
		hook.logger.Error(message.String(),
			zap.String("trace_id", traceId),
			zap.String("span_id", spanId),
			zap.String("exception_msg", cmds[0].Err().Error()),
			zap.String("exception_type", "redis"),
		)
	} else {
		hook.logger.Info(message.String(),
			zap.String("trace_id", traceId),
			zap.String("span_id", spanId),
		)
	}
	return nil
}

type hookMetric struct {
	dsn             *DSN
	requestsTotals  *prometheus.CounterVec
	requestsLatency *prometheus.HistogramVec
}

type metricTimeKey struct{}

func (hook *hookMetric) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return ctx, nil
	}
	return context.WithValue(ctx, metricTimeKey{}, time.Now()), nil
}

func (hook *hookMetric) AfterProcess(ctx context.Context, _ redis.Cmder) error {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return nil
	}
	st, ok := ctx.Value(metricTimeKey{}).(time.Time)
	if !ok {
		return nil
	}
	instance := strings.Join(hook.dsn.Addr, ",")
	dbStr := strconv.FormatInt(int64(hook.dsn.DB), 10)

	hook.requestsTotals.WithLabelValues(instance, dbStr, operation).Inc()
	hook.requestsLatency.WithLabelValues(instance, dbStr, operation).Observe(time.Since(st).Seconds())
	return nil
}

func (hook *hookMetric) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return ctx, nil
	}
	return context.WithValue(ctx, metricTimeKey{}, time.Now()), nil
}

func (hook *hookMetric) AfterProcessPipeline(ctx context.Context, _ []redis.Cmder) error {
	var operation string
	if operation = OperationFrom(ctx); operation == "" {
		return nil
	}
	st, ok := ctx.Value(metricTimeKey{}).(time.Time)
	if !ok {
		return nil
	}
	instance := strings.Join(hook.dsn.Addr, ",")
	dbStr := strconv.FormatInt(int64(hook.dsn.DB), 10)

	hook.requestsTotals.WithLabelValues(instance, dbStr, operation).Inc()
	hook.requestsLatency.WithLabelValues(instance, dbStr, operation).Observe(time.Since(st).Seconds())
	return nil
}

func IsActualError(err error) bool {
	return err != nil && err != redis.Nil
}
