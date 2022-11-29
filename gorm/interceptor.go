package gorm

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/semconv/v1.6.1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strings"
	"time"
)

type Processor interface {
	Get(name string) func(*gorm.DB)
	Replace(name string, handler func(*gorm.DB)) error
}

type Handler func(*gorm.DB)

type Interceptor func(operation string, next Handler) Handler

func InterceptorTracing(dsn *DSN) Interceptor {
	tracer := otel.Tracer(dsn.Driver)

	return func(action string, next Handler) Handler {
		return func(db *gorm.DB) {
			var (
				ctx       context.Context
				operation string
			)
			if ctx = db.Statement.Context; ctx == nil {
				next(db)
				return
			}
			if operation = OperationFrom(ctx); operation == "" {
				next(db)
				return
			}

			ctx, span := tracer.Start(ctx, operation)
			defer span.End()

			span.SetAttributes(semconv.DBSystemKey.String(dsn.Driver))
			span.SetAttributes(semconv.DBConnectionStringKey.String(dsn.Addr))
			span.SetAttributes(semconv.DBUserKey.String(dsn.Username))
			span.SetAttributes(semconv.DBNameKey.String(dsn.DbName))
			span.SetAttributes(semconv.DBStatementKey.String(db.Statement.SQL.String()))
			span.SetAttributes(semconv.DBOperationKey.String(operation))
			span.SetAttributes(attribute.Key("trace_id").String(trace.SpanContextFromContext(ctx).TraceID().String()))

			next(db)

			if err := db.Statement.Error; err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "OK")
			}
		}
	}
}

func InterceptorLogging(dsn *DSN, logger *zap.Logger) Interceptor {
	return func(action string, next Handler) Handler {
		return func(db *gorm.DB) {
			var (
				ctx       context.Context
				operation string
				st        = time.Now()
			)
			if ctx = db.Statement.Context; ctx == nil {
				next(db)
				return
			}
			if operation = OperationFrom(ctx); operation == "" {
				next(db)
				return
			}

			next(db)

			var (
				latency = time.Since(st)
				message strings.Builder
			)

			message.WriteString("db.operation=" + operation)
			message.WriteString("\t")
			message.WriteString("db.system=" + dsn.Driver)
			message.WriteString("\t")
			message.WriteString("db.connection_string=" + dsn.Addr)
			message.WriteString("\t")
			message.WriteString("db.user=" + dsn.Username)
			message.WriteString("\t")
			message.WriteString("db.name=" + dsn.DbName)
			message.WriteString("\t")
			message.WriteString("db.statement=" + detailSQL(db))
			message.WriteString("\t")
			message.WriteString("latency=" + latency.String())
			message.WriteString("\t")

			traceId := trace.SpanContextFromContext(ctx).TraceID().String()
			spanId := trace.SpanContextFromContext(ctx).SpanID().String()

			if err := db.Statement.Error; err != nil {
				logger.Error(message.String(),
					zap.String("trace_id", traceId),
					zap.String("span_id", spanId),
					zap.String("exception_msg", err.Error()),
					zap.String("exception_type", "gorm"),
				)
			} else {
				logger.Info(message.String(),
					zap.String("trace_id", traceId),
					zap.String("span_id", spanId),
				)
			}
		}
	}
}

func InterceptorMetrics(dsn *DSN) Interceptor {
	requestsTotals := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "db",
		Subsystem: "requests",
		Name:      "totals",
		Help:      "The total number of db operation",
	}, []string{"db_instance", "db_name", "operation"})

	requestLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "db",
		Subsystem:   "requests",
		Name:        "latency_seconds",
		Help:        "The second latency of db operation",
		ConstLabels: nil,
	}, []string{"db_instance", "db_name", "operation"})
	prometheus.MustRegister(requestsTotals, requestLatency)

	return func(action string, next Handler) Handler {
		return func(db *gorm.DB) {
			var (
				ctx       context.Context
				operation string
				st        = time.Now()
			)
			if ctx = db.Statement.Context; ctx == nil {
				next(db)
				return
			}
			if operation = OperationFrom(ctx); operation == "" {
				next(db)
				return
			}

			next(db)

			if db.Statement.Error == nil {
				requestsTotals.WithLabelValues(dsn.Addr, dsn.DbName, operation).Inc()
				requestLatency.WithLabelValues(dsn.Addr, dsn.DbName, operation).Observe(time.Since(st).Seconds())
			} else {
				requestsTotals.WithLabelValues(dsn.Addr, dsn.DbName, operation).Inc()
				requestLatency.WithLabelValues(dsn.Addr, dsn.DbName, operation).Observe(time.Since(st).Seconds())
			}
		}
	}
}

func detailSQL(db *gorm.DB) string {
	return db.Explain(db.Statement.SQL.String(), db.Statement.Vars...)
}
