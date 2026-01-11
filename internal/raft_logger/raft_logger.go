package raftlogger

import (
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/rs/zerolog"
)

type RaftZerologAdapter struct {
	zlog *zerolog.Logger
}

func NewRaftZerologAdapter(logger *zerolog.Logger) *RaftZerologAdapter {
	return &RaftZerologAdapter{
		zlog: logger,
	}
}

func (a *RaftZerologAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	var event *zerolog.Event
	switch level {
	case hclog.Trace:
		event = a.zlog.Trace()
	case hclog.Debug:
		event = a.zlog.Debug()
	case hclog.Info:
		event = a.zlog.Info()
	case hclog.Warn:
		event = a.zlog.Warn()
	case hclog.Error:
		event = a.zlog.Error()
	default:
		event = a.zlog.Info()
	}

	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key, ok := args[i].(string)
			if !ok {
				key = fmt.Sprintf("%v", args[i])
			}
			event = event.Interface(key, args[i+1])
		}
	}

	event.Msg(msg)
}

func (a *RaftZerologAdapter) Trace(msg string, args ...interface{}) {
	a.Log(hclog.Trace, msg, args...)
}

func (a *RaftZerologAdapter) Debug(msg string, args ...interface{}) {
	a.Log(hclog.Debug, msg, args...)
}

func (a *RaftZerologAdapter) Info(msg string, args ...interface{}) {
	a.Log(hclog.Info, msg, args...)
}

func (a *RaftZerologAdapter) Warn(msg string, args ...interface{}) {
	a.Log(hclog.Warn, msg, args...)
}

func (a *RaftZerologAdapter) Error(msg string, args ...interface{}) {
	a.Log(hclog.Error, msg, args...)
}

func (a *RaftZerologAdapter) IsTrace() bool {
	return a.zlog.GetLevel() <= zerolog.TraceLevel
}

func (a *RaftZerologAdapter) IsDebug() bool {
	return a.zlog.GetLevel() <= zerolog.DebugLevel
}

func (a *RaftZerologAdapter) IsInfo() bool {
	return a.zlog.GetLevel() <= zerolog.InfoLevel
}

func (a *RaftZerologAdapter) IsWarn() bool {
	return a.zlog.GetLevel() <= zerolog.WarnLevel
}

func (a *RaftZerologAdapter) IsError() bool {
	return a.zlog.GetLevel() <= zerolog.ErrorLevel
}

func (a *RaftZerologAdapter) ImpliedArgs() []interface{} {
	return nil
}

func (a *RaftZerologAdapter) With(args ...interface{}) hclog.Logger {
	contextLogger := a.zlog.With()
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key, ok := args[i].(string)
			if !ok {
				key = fmt.Sprintf("%v", args[i])
			}
			contextLogger = contextLogger.Interface(key, args[i+1])
		}
	}
	newLogger := contextLogger.Logger()
	return &RaftZerologAdapter{
		zlog: &newLogger,
	}
}

func (a *RaftZerologAdapter) Name() string {
	return "raft"
}

func (a *RaftZerologAdapter) SetLevel(level hclog.Level) {}

func (a *RaftZerologAdapter) GetLevel() hclog.Level {
	switch a.zlog.GetLevel() {
	case zerolog.TraceLevel:
		return hclog.Trace
	case zerolog.DebugLevel:
		return hclog.Debug
	case zerolog.InfoLevel:
		return hclog.Info
	case zerolog.WarnLevel:
		return hclog.Warn
	case zerolog.ErrorLevel:
		return hclog.Error
	default:
		return hclog.Info
	}
}

func (a *RaftZerologAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return nil
}

func (a *RaftZerologAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return nil
}
func (a *RaftZerologAdapter) Named(name string) hclog.Logger {
	return nil
}

func (a *RaftZerologAdapter) ResetNamed(name string) hclog.Logger {
	return nil
}
