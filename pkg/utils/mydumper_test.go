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
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

func (t *testUtilsSuite) TestParseMetaData(c *C) {
	f, err := ioutil.TempFile("", "metadata")
	c.Assert(err, IsNil)
	defer os.Remove(f.Name())

	testCases := []struct {
		source string
		pos    *mysql.Position
	}{
		{
			`Started dump at: 2018-12-28 07:20:49
SHOW MASTER STATUS:
        Log: bin.000001
        Pos: 2479
        GTID:97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2018-12-28 07:20:51`,
			&mysql.Position{
				Name: "bin.000001",
				Pos:  2479,
			},
		},
		{
			`Started dump at: 2018-12-27 19:51:22
SHOW MASTER STATUS:
        Log: mysql-bin.000003
        Pos: 3295817
        GTID:

SHOW SLAVE STATUS:
        Host: 10.128.27.98
        Log: mysql-bin.000003
        Pos: 329635
        GTID:

Finished dump at: 2018-12-27 19:51:22`,
			&mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  3295817,
			},
		},
	}

	for _, tc := range testCases {
		err := ioutil.WriteFile(f.Name(), []byte(tc.source), 0644)
		c.Assert(err, IsNil)
		pos, err := ParseMetaData(f.Name())
		c.Assert(err, IsNil)
		c.Assert(pos, DeepEquals, tc.pos)
	}
}
