// Copyright 2015 - 2017 Ka-Hing Cheung
//
// Licensed under the Apache License, version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"io"
	glog "log"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var mu sync.Mutex
var loggers = make(map[string]*LogHandle)

var StdouterrLogger = GetLogger("main")
var log = StdouterrLogger

type LogHandle struct {
	logrus.Logger

	name string
	Lvl  *logrus.Level
}

func (l *LogHandle) Format(e *logrus.Entry) ([]byte, error) {
	// Mon Jan 2 15:04:05 -0700 MST 2006
	timestamp := ""
	lvl := e.Level
	if l.Lvl != nil {
		lvl = *l.Lvl
	}

	const timeFormat = "2006/01/02 15:04:05.000000"
	timestamp = e.Time.Format(timeFormat) + " "

	str := fmt.Sprintf("%v%v.%v %v",
		timestamp,
		l.name,
		strings.ToUpper(lvl.String()),
		e.Message)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}

	str += "\n"
	return []byte(str), nil
}

// for aws.Logger
func (l *LogHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

func NewLogger(name string) *LogHandle {
	l := &LogHandle{name: name}
	l.Out = os.Stderr
	l.Formatter = l
	l.Level = logrus.InfoLevel
	l.Hooks = make(logrus.LevelHooks)
	l.Infof("NewLogger, Name=%v", name)
	return l
}

func NewLoggerToFile(name string, fname string) *LogHandle {
	if fname == "" {
		return NewLogger(name)
	}
	stat, err := os.Stat(fname)
	if err == nil {
		fsplit := strings.Split(fname, ".")
		var newName = ""
		if len(fsplit) > 1 {
			newName = fmt.Sprintf("%s.%s.%s", strings.Join(fsplit[:len(fsplit)-1], "."), stat.ModTime().Format("060102-150405"), fsplit[len(fsplit)-1])
		} else {
			newName = fmt.Sprintf("%s.%s", fname, stat.ModTime().Format("YYMMDD-hhmmss"))
		}
		if err := os.Rename(fname, newName); err != nil {
			log.Fatalf("Failed: NewLoggerToFile, Rename, fname=%v, newName=%v", fname, newName)
		}
		log.Infof("NewLoggerToFile, Rename, fname=%v->%v", fname, newName)
	} else if !os.IsNotExist(err) {
		log.Fatalf("Failed: NewLoggerToFile, Stat, fname=%v, err=%v", fname, err)
	}
	l := &LogHandle{name: name}
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed: NewLoggerToFile, OpenFile, fname=%v, err=%v", fname, err)
	}
	l.Out = io.WriteCloser(f)
	l.Formatter = l
	l.Level = logrus.InfoLevel
	l.Hooks = make(logrus.LevelHooks)
	l.Infof("NewLoggerToFile, Name=%v, fname=%v", name, fname)
	return l
}

func GetLogger(name string) *LogHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		if name != "main" && name != "fuse" && name != "objcache" {
			logger.Level = logrus.InfoLevel
		}
		return logger
	} else {
		if old, ok := loggers[name]; ok {
			if closer, ok2 := old.Out.(io.WriteCloser); ok2 {
				closer.Close()
			}
		}
		logger := NewLogger(name)
		loggers[name] = logger
		if name != "main" && name != "fuse" && name != "objcache" {
			logger.Level = logrus.InfoLevel
		}
		return logger
	}
}

func GetLoggerFile(name string, fname string) *LogHandle {
	mu.Lock()
	defer mu.Unlock()

	if old, ok := loggers[name]; ok {
		if closer, ok2 := old.Out.(io.WriteCloser); ok2 {
			closer.Close()
		}
	}
	logger := NewLoggerToFile(name, fname)
	loggers[name] = logger
	if name != "main" && name != "fuse" && name != "objcache" {
		logger.Level = logrus.InfoLevel
	}
	return logger
}

func GetStdLogger(l *LogHandle, lvl logrus.Level) *glog.Logger {
	return glog.New(l.WriterLevel(lvl), "", 0)
}
