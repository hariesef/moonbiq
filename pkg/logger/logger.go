package logger

import (
	"fmt"
	"log"
	"os"
)

// Logger is the wrapper around the standard logger that provides leveled logging
type Logger struct {
	*log.Logger
}

// Global logger instance
var std = &Logger{log.New(os.Stdout, "", log.LstdFlags)}

// LogLevel represents the logging level
type LogLevel int

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in production
	DebugLevel LogLevel = iota
	// InfoLevel is the default logging priority
	InfoLevel
	// WarnLevel logs are more important than Info
	WarnLevel
	// ErrorLevel logs are high-priority and should be looked at immediately
	ErrorLevel
)

var levelNames = map[LogLevel]string{
	DebugLevel: "DEBUG",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
}

var currentLevel = InfoLevel

// Initialize sets up the logger with the specified level
func Initialize(level string) {
	switch level {
	case "debug":
		currentLevel = DebugLevel
		std.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	case "info":
		currentLevel = InfoLevel
		std.SetFlags(log.Ldate | log.Ltime)
	case "warn":
		currentLevel = WarnLevel
		std.SetFlags(log.Ldate | log.Ltime)
	case "error":
		currentLevel = ErrorLevel
		std.SetFlags(log.Ldate | log.Ltime)
	default:
		currentLevel = DebugLevel
		std.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	}
}

func (l *Logger) log(level LogLevel, format string, v ...interface{}) {
	if level < currentLevel {
		return
	}

	prefix := fmt.Sprintf("[%s] ", levelNames[level])
	l.SetPrefix(prefix)
	l.Output(3, fmt.Sprintf(format, v...))
}

// Debug logs a message at DebugLevel
func Debug(format string, v ...interface{}) {
	std.log(DebugLevel, format, v...)
}

// Info logs a message at InfoLevel
func Info(format string, v ...interface{}) {
	std.log(InfoLevel, format, v...)
}

// Warn logs a message at WarnLevel
func Warn(format string, v ...interface{}) {
	std.log(WarnLevel, format, v...)
}

// Error logs a message at ErrorLevel
func Error(format string, v ...interface{}) {
	std.log(ErrorLevel, format, v...)
}
