// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"bytes"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogLevel      = log.InfoLevel
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
)

func init() {
	log.SetLevel(defaultLogLevel)
	log.AddHook(&contextHook{})
	log.SetFormatter(&textFormatter{})
}

// SetLevelByString sets log's level by a level string.
func SetLevelByString(level string) {
	log.SetLevel(stringToLogLevel(level))
}

// GetLogLevelAsString gets current log's level as a level string.
func GetLogLevelAsString() string {
	return log.GetLevel().String()
}

// SetOutputByName sets the filename for the log.
func SetOutputByName(filename string) {
	output := &lumberjack.Logger{
		Filename:  filename,
		LocalTime: true, // use default `MaxSize` (100M) now.
	}
	log.SetOutput(output)
}

// Info logs a message at level Info on the wrapped logger.
func Info(v ...interface{}) {
	log.Info(v...)
}

// Infof logs a message at level Info on the wrapped logger.
func Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

// Debug logs a message at level Debug on the wrapped logger.
func Debug(v ...interface{}) {
	log.Debug(v...)
}

// Debugf logs a message at level Debug on the wrapped logger.
func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

// Warn logs a message at level Warn on the wrapped logger.
func Warn(v ...interface{}) {
	log.Warn(v...)
}

// Warnf logs a message at level Warn on the wrapped logger.
func Warnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

// Error logs a message at level Error on the wrapped logger.
func Error(v ...interface{}) {
	log.Error(v...)
}

// Errorf logs a message at level Error on the wrapped logger.
func Errorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

// Fatal logs a message at level Fatal on the wrapped logger then the process will exit with status set to 1.
func Fatal(v ...interface{}) {
	log.Fatal(v...)
}

// Fatalf logs a message at level Fatal on the wrapped logger then the process will exit with status set to 1.
func Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

func stringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	// these two num are set by manually testing
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(10, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/pingcap/dm/pkg/log")
}

// textFormatter is for compatibility with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
	EnableEntryOrder bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	if f.EnableEntryOrder {
		keys := make([]string, 0, len(entry.Data))
		for k := range entry.Data {
			if k != "file" && k != "line" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, " %v=%v", k, entry.Data[k])
		}
	} else {
		for k, v := range entry.Data {
			if k != "file" && k != "line" {
				fmt.Fprintf(b, " %v=%v", k, v)
			}
		}
	}

	b.WriteByte('\n')

	return b.Bytes(), nil
}
