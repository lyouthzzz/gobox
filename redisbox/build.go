package redisbox

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

//go:generate protoc  --proto_path=. --go_out=paths=source_relative:.  --go-grpc_out=paths=source_relative:. config.proto

func (x *Config) Build(opts ...Option) (*redis.Client, error) {
	options := &options{logger: globalLogger}
	for _, opt := range opts {
		opt(options)
	}
	client := redis.NewClient(&redis.Options{Addr: x.Addr[0], Password: x.Password, DB: int(x.Db)})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	dsn := &DSN{Addr: x.Addr, DB: int(x.Db)}

	client.AddHook(newHookTrace(dsn))
	client.AddHook(newHookLogger(dsn, options.logger))
	client.AddHook(newHookMetric(dsn))

	return client, nil
}

func (x *Config) BuildMust(opts ...Option) *redis.Client {
	client, err := x.Build(opts...)
	if err != nil {
		panic(err)
	}
	return client
}

func DefaultConfig() *Config {
	return &Config{}
}

func (x *Config) WithAddr(addr ...string) *Config {
	x.Addr = addr
	return x
}

func (x *Config) WithPassword(pwd string) *Config {
	x.Password = pwd
	return x
}

func (x *Config) WithDB(db int) *Config {
	x.Db = int32(db)
	return x
}
