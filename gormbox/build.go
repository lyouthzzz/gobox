package gormbox

import (
	"gorm.io/gorm"
)

//go:generate protoc  --proto_path=. --go_out=paths=source_relative:.  --go-grpc_out=paths=source_relative:. config.proto

func (x *Config) Build(opts ...Option) (*gorm.DB, error) {
	options := &options{logger: globalLogger}
	for _, opt := range opts {
		opt(options)
	}

	if x.Driver == "" {
		x.Driver = DriverMysql
	}
	parser := GetParser(x.Driver)

	dsn, err := parser.ParseDSN(x.Dsn)
	if err != nil {
		return nil, err
	}
	db, err := gorm.Open(parser.GetDialector(x.Dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	ints := make([]Interceptor, 0)
	ints = append(ints, InterceptorTracing(dsn), InterceptorLogging(dsn, options.logger), InterceptorMetrics(dsn))

	replace := func(processor Processor, callbackName string, interceptors ...Interceptor) {
		handler := processor.Get(callbackName)
		for _, interceptor := range ints {
			handler = interceptor(callbackName, handler)
		}
		_ = processor.Replace(callbackName, handler)
	}
	replace(db.Callback().Create(), "gorm:create", ints...)
	replace(db.Callback().Update(), "gorm:update", ints...)
	replace(db.Callback().Delete(), "gorm:delete", ints...)
	replace(db.Callback().Query(), "gorm:query", ints...)
	replace(db.Callback().Raw(), "gorm:raw", ints...)

	return db, nil
}

func (x *Config) BuildMust(opts ...Option) *gorm.DB {
	db, err := x.Build(opts...)
	if err != nil {
		panic(err)
	}
	return db
}

func DefaultConfig() *Config {
	return &Config{}
}

func (x *Config) WithDriver(driver string) *Config {
	x.Driver = driver
	return x
}

func (x *Config) WithDSN(dsn string) *Config {
	x.Dsn = dsn
	return x
}
