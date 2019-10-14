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

package streamer

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/siddontang/go-mysql-elasticsearch/pkg/log"
	"github.com/siddontang/go-mysql-elasticsearch/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
)

var (
	// ErrInvalidBinlogFilename means error about invalid binlog file name.
	ErrInvalidBinlogFilename = errors.New("invalid binlog file name")
	// ErrEmptyRelayDir means error about empty relay dir.
	ErrEmptyRelayDir = errors.New("empty relay dir")
	// binlog file name, base + `.` + seq
	baseSeqSeparator = "."
)

// FileCmp is a compare condition used when collecting binlog files
type FileCmp uint8

// FileCmpLess represents a < FileCmp condition, others are similar
const (
	FileCmpLess FileCmp = iota + 1
	FileCmpLessEqual
	FileCmpEqual
	FileCmpBiggerEqual
	FileCmpBigger
)

// CollectAllBinlogFiles collects all valid binlog files in dir
func CollectAllBinlogFiles(dir string) ([]string, error) {
	if dir == "" {
		return nil, ErrEmptyRelayDir
	}
	files, err := readDir(dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := make([]string, 0, len(files))
	for _, f := range files {
		if strings.HasPrefix(f, utils.MetaFilename) {
			// skip meta file or temp meta file
			log.Debugf("[streamer] skip meta file %s", f)
			continue
		}
		_, err := parseBinlogFile(f)
		if err != nil {
			log.Warnf("[streamer] collecting binlog file, ignore invalid file %s, err %v", f, err)
			continue
		}
		ret = append(ret, f)
	}
	return ret, nil
}

// CollectBinlogFilesCmp collects valid binlog files with a compare condition
func CollectBinlogFilesCmp(dir, baseFile string, cmp FileCmp) ([]string, error) {
	if dir == "" {
		return nil, ErrEmptyRelayDir
	}

	if bp := filepath.Join(dir, baseFile); !utils.IsFileExists(bp) {
		return nil, errors.NotFoundf("base file %s in directory %s", baseFile, dir)
	}

	bf, err := parseBinlogFile(baseFile)
	if err != nil {
		return nil, errors.Annotatef(err, "filename %s", baseFile)
	}

	allFiles, err := CollectAllBinlogFiles(dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	results := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		// we have parse f in `CollectAllBinlogFiles`, may be we can refine this
		parsed, err := parseBinlogFile(f)
		if err != nil || parsed.baseName != bf.baseName {
			log.Warnf("[streamer] collecting binlog file, ignore invalid file %s, err %v", f, err)
			continue
		}
		switch cmp {
		case FileCmpBigger:
			if !parsed.BiggerThan(bf) {
				log.Debugf("[streamer] ignore older or equal binlog file %s in dir %s", f, dir)
				continue
			}
		case FileCmpBiggerEqual:
			if !parsed.BiggerOrEqualThan(bf) {
				log.Debugf("[streamer] ignore older binlog file %s in dir %s", f, dir)
				continue
			}
		case FileCmpLess:
			if !parsed.LessThan(bf) {
				log.Debugf("[streamer] ignore newer or equal binlog file %s in dir %s", f, dir)
				continue
			}
		default:
			return nil, errors.NotSupportedf("cmp condition %v", cmp)
		}

		results = append(results, f)
	}

	return results, nil
}

// GetBinlogFileIndex return a float64 value of index.
func GetBinlogFileIndex(filename string) (float64, error) {
	f, err := parseBinlogFile(filename)
	if err != nil {
		return 0, errors.Errorf("parse binlog file name %s err %v", filename, err)
	}

	idx, err := strconv.ParseFloat(f.seq, 64)
	if err != nil {
		return 0, errors.Errorf("parse binlog index %s to float64, err %v", filename, err)
	}
	return idx, nil
}

func parseBinlogFile(filename string) (*binlogFile, error) {
	// chendahui: I found there will always be only one dot in the mysql binlog name.
	parts := strings.Split(filename, baseSeqSeparator)
	if len(parts) != 2 || !allAreDigits(parts[1]) {
		return nil, ErrInvalidBinlogFilename
	}

	return &binlogFile{
		baseName: parts[0],
		seq:      parts[1],
	}, nil
}

func constructBinlogFilename(baseName, seq string) string {
	return fmt.Sprintf("%s%s%s", baseName, baseSeqSeparator, seq)
}

// RealMySQLPos parses relay position and returns a mysql position and whether error occurs
// if parsed successfully and `UUIDSuffix` exists, sets position Name to
// `originalPos.NamePrefix + baseSeqSeparator + originalPos.NameSuffix`.
// if parsed failed returns given position and the traced error.
func RealMySQLPos(pos mysql.Position) (mysql.Position, error) {
	parsed, err := parseBinlogFile(pos.Name)
	if err != nil {
		return pos, errors.Trace(err)
	}

	sepIdx := strings.Index(parsed.baseName, posUUIDSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posUUIDSuffixSeparator) < len(parsed.baseName) {
		return mysql.Position{
			Name: fmt.Sprintf("%s%s%s", parsed.baseName[:sepIdx], baseSeqSeparator, parsed.seq),
			Pos:  pos.Pos,
		}, nil
	}

	return pos, nil
}

type binlogFile struct {
	baseName string
	seq      string
}

func (f *binlogFile) BiggerThan(other *binlogFile) bool {
	return f.baseName == other.baseName && f.seq > other.seq
}

func (f *binlogFile) BiggerOrEqualThan(other *binlogFile) bool {
	return f.baseName == other.baseName && f.seq >= other.seq
}

func (f *binlogFile) LessThan(other *binlogFile) bool {
	return f.baseName == other.baseName && f.seq < other.seq
}

// [0-9] in string -> [48,57] in ascii
func allAreDigits(s string) bool {
	for _, r := range s {
		if r >= 48 && r <= 57 {
			continue
		}
		return false
	}
	return true
}

// readDir reads and returns all file(sorted asc) and dir names from directory f
func readDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, errors.Annotatef(err, "dir %s", dirpath)
	}

	sort.Strings(names)

	return names, nil
}
