package gormbox

import (
	"github.com/go-sql-driver/mysql"
	"gorm.io/driver/clickhouse"
	gormysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net/url"
)

type DSN struct {
	Driver   string // mysql driver or clickhouse driver
	Net      string // net protocol
	Addr     string // connect address
	Username string // connect username
	Password string // connect password
	DbName   string // connect db
}

type Parser interface {
	ParseDSN(string) (*DSN, error)
	GetDialector(string) gorm.Dialector
}

var (
	_ Parser = (*mysqlParser)(nil)
	_ Parser = (*clickhouseParser)(nil)
)

const (
	DriverMysql      = "mysql"
	DriverClickhouse = "clickhouse"
)

var parsers = make(map[string]Parser)

func init() {
	parsers[DriverClickhouse] = &clickhouseParser{}
	parsers[DriverMysql] = &mysqlParser{}
}

func GetParser(driver string) Parser {
	return parsers[driver]
}

type mysqlParser struct{}

func (parser *mysqlParser) GetDialector(dsn string) gorm.Dialector {
	return gormysql.Open(dsn)
}

func (parser *mysqlParser) ParseDSN(dsn string) (*DSN, error) {
	// dsn example: user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local
	c, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &DSN{Driver: DriverMysql, Net: c.Net, Addr: c.Addr, Username: c.User, Password: c.Passwd, DbName: c.DBName}, nil
}

type clickhouseParser struct{}

func (parser *clickhouseParser) GetDialector(dsn string) gorm.Dialector {
	return clickhouse.Open(dsn)
}

func (parser *clickhouseParser) ParseDSN(dsn string) (*DSN, error) {
	// clickhouse dsn example: tcp://host:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	cfg := &DSN{Driver: DriverClickhouse}

	cfg.Net, cfg.Addr = u.Scheme, u.Host
	if len(u.Path) > 1 {
		// skip '/'
		cfg.DbName = u.Path[1:]
	}
	if u.User != nil {
		// it is expected that empty password will be dropped out on Parse and Format
		cfg.Username = u.User.Username()
	}
	if err = parser.parseDSNParams(cfg, map[string][]string(u.Query())); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (parser *clickhouseParser) parseDSNParams(cfg *DSN, params map[string][]string) error {
	for k, v := range params {
		if len(v) == 0 {
			continue
		}
		switch k {
		case "username":
			cfg.Username = v[0]
		case "database":
			cfg.DbName = v[0]
		}
	}
	return nil
}
