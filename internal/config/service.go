package config

import (
	"io"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

const FormatJSON = "json"

type ServiceConfig struct {
	LogLevel     string `envconfig:"LOG_LEVEL" default:"trace"`
	LogFormat    string `envconfig:"LOG_FORMAT" default:"console"`
	ReportCaller bool   `envconfig:"LOG_REPORT_CALLER" default:"false"`

	InsecureSkipVerify bool `envconfig:"INSECURE_SKIP_VERIFY" default:"true"`

	Bind       string `envconfig:"BIND_SERVICE" default:":9000"`
	HealthBind string `envconfig:"BIND_HEALTH" default:":9091"`
}

var service *ServiceConfig

func Service() ServiceConfig {

	if service != nil {
		return *service
	}
	service = &ServiceConfig{}
	if err := envconfig.Process("", service); err != nil {
		panic(err)
	}
	if len(service.Bind) > 0 && service.Bind[0] == ':' {
		service.Bind = "127.0.0.1" + service.Bind
	}
	return *service
}

func (cfg ServiceConfig) Logger() (logger zerolog.Logger) {

	level := zerolog.InfoLevel
	if newLevel, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		level = newLevel
	}
	var out io.Writer = os.Stdout
	if cfg.LogFormat != FormatJSON {
		out = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.StampMicro}
	}
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	ctxLog := zerolog.New(out).Level(level).With().Timestamp().Stack()
	if cfg.ReportCaller {
		ctxLog = ctxLog.Caller()
	}
	return ctxLog.Logger()
}
