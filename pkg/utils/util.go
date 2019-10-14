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

package utils

import (
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
)

// DecodeBinlogPosition parses a mysql.Position from string format
func DecodeBinlogPosition(pos string) (*mysql.Position, error) {
	invalidFmt := "invalid mysql position string: %s"
	if len(pos) < 3 {
		return nil, errors.Errorf(invalidFmt, pos)
	}
	if pos[0] != '(' || pos[len(pos)-1] != ')' {
		return nil, errors.Errorf(invalidFmt, pos)
	}
	sp := strings.Split(pos[1:len(pos)-1], ",")
	if len(sp) != 2 {
		return nil, errors.Errorf(invalidFmt, pos)
	}
	position, err := strconv.ParseUint(strings.TrimSpace(sp[1]), 10, 32)
	if err != nil {
		return nil, errors.Errorf(invalidFmt, pos)
	}
	return &mysql.Position{
		Name: strings.TrimSpace(sp[0]),
		Pos:  uint32(position),
	}, nil
}

// CompareBinlogPos compares binlog positions.
// The result will be 0 if |a-b| < deviation, otherwise -1 if a < b, and +1 if a > b.
func CompareBinlogPos(a, b mysql.Position, deviation float64) int {
	if a.Name < b.Name {
		return -1
	}

	if a.Name > b.Name {
		return 1
	}

	if math.Abs(float64(a.Pos)-float64(b.Pos)) <= deviation {
		return 0
	}

	if a.Pos < b.Pos {
		return -1
	}

	return 1
}
