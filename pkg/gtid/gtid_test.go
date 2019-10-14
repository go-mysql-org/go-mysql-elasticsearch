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

package gtid

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testGTIDSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testGTIDSuite struct {
}

func (s *testGTIDSuite) TestGTID(c *C) {
	matserUUIDs := []string{
		"53ea0ed1-9bf8-11e6-8bea-64006a897c73",
		"53ea0ed1-9bf8-11e6-8bea-64006a897c72",
		"53ea0ed1-9bf8-11e6-8bea-64006a897c71",
	}

	cases := []struct {
		flavor        string
		masterIDs     []interface{}
		selfGTIDstr   string
		masterGTIDStr string
		exepctedStr   string
	}{
		{"mariadb", []interface{}{uint32(1)}, "1-1-1,2-2-2", "1-1-12,4-4-4", "1-1-1,4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "2-2-2", "1-2-12,2-2-3,4-4-4", "2-2-2,4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "", "1-1-12,4-4-4", "4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "2-2-2", "", ""},
		{"mariadb", []interface{}{uint32(1), uint32(3)}, "1-1-1,3-3-4,2-2-2", "1-1-12,3-3-8,4-4-4", "1-1-1,3-3-4,4-4-4"},
		{"mariadb", []interface{}{uint32(1), uint32(3)}, "2-2-2", "1-2-12,2-2-3,3-3-8,4-4-4", "2-2-2,4-4-4"},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[1], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0]}, "", fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-4", matserUUIDs[1])},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), "", ""},
		{"mysql", []interface{}{matserUUIDs[0], matserUUIDs[1]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0], matserUUIDs[2]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2", matserUUIDs[1])},
	}

	for _, cs := range cases {
		selfGTIDSet, err := ParserGTID(cs.flavor, cs.selfGTIDstr)
		c.Assert(err, IsNil)
		newGTIDSet, err := ParserGTID(cs.flavor, cs.masterGTIDStr)
		c.Assert(err, IsNil)
		excepted, err := ParserGTID(cs.flavor, cs.exepctedStr)
		c.Assert(err, IsNil)

		err = selfGTIDSet.Replace(newGTIDSet, cs.masterIDs)
		c.Logf("%s %s %s", selfGTIDSet, newGTIDSet, excepted)
		c.Assert(err, IsNil)
		c.Assert(selfGTIDSet.Origin().Equal(excepted.Origin()), IsTrue)
		c.Assert(newGTIDSet.Origin().Equal(excepted.Origin()), IsTrue)
	}
}

func (s *testGTIDSuite) TestMySQLGTIDEqual(c *C) {
	var (
		g1     *mySQLGTIDSet
		g2     *mySQLGTIDSet
		gMaria *mariadbGTIDSet
	)

	c.Assert(g1.Equal(nil), IsTrue)
	c.Assert(g1.Equal(g2), IsTrue)
	c.Assert(g1.Equal(gMaria), IsFalse)

	gSet, err := ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454")
	c.Assert(err, IsNil)
	g1 = gSet.(*mySQLGTIDSet)
	c.Assert(g1.Equal(g2), IsFalse)

	gSet, err = ParserGTID("mysql", "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454,3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190")
	c.Assert(err, IsNil)
	g2 = gSet.(*mySQLGTIDSet)
	c.Assert(g1.Equal(g2), IsTrue)
}

func (s *testGTIDSuite) TestMariaGTIDEqual(c *C) {
	var (
		g1     *mariadbGTIDSet
		g2     *mariadbGTIDSet
		gMySQL *mySQLGTIDSet
	)

	c.Assert(g1.Equal(nil), IsTrue)
	c.Assert(g1.Equal(g2), IsTrue)
	c.Assert(g1.Equal(gMySQL), IsFalse)

	gSet, err := ParserGTID("mariadb", "1-1-1,2-2-2")
	c.Assert(err, IsNil)
	g1 = gSet.(*mariadbGTIDSet)
	c.Assert(g1.Equal(g2), IsFalse)

	gSet, err = ParserGTID("mariadb", "2-2-2,1-1-1")
	c.Assert(err, IsNil)
	g2 = gSet.(*mariadbGTIDSet)
	c.Assert(g1.Equal(g2), IsTrue)
}

func (s *testGTIDSuite) TestMySQLGTIDContain(c *C) {
	var (
		g1     *mySQLGTIDSet
		g2     *mySQLGTIDSet
		gMaria *mariadbGTIDSet
	)
	c.Assert(g1.Contain(g2), IsTrue)      // all nil
	c.Assert(g1.Contain(gMaria), IsFalse) // incompatible

	// one nil
	gSet, err := ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-10,406a3f61-690d-11e7-87c5-6c92bf46f384:1-10")
	c.Assert(err, IsNil)
	g1 = gSet.(*mySQLGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// contain
	gSet, err = ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-5,406a3f61-690d-11e7-87c5-6c92bf46f384:1-10")
	c.Assert(err, IsNil)
	g2 = gSet.(*mySQLGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// not contain
	gSet, err = ParserGTID("mysql", "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-5,406a3f61-690d-11e7-87c5-6c92bf46f384:1-10")
	c.Assert(err, IsNil)
	g2 = gSet.(*mySQLGTIDSet)
	c.Assert(g1.Contain(g2), IsFalse)
	c.Assert(g2.Contain(g1), IsFalse)
}

func (s *testGTIDSuite) TestMairaGTIDContain(c *C) {
	var (
		g1     *mariadbGTIDSet
		g2     *mariadbGTIDSet
		gMySQL *mySQLGTIDSet
	)
	c.Assert(g1.Contain(g2), IsTrue)      // all nil
	c.Assert(g1.Contain(gMySQL), IsFalse) // incompatible

	// one nil
	gSet, err := ParserGTID("mariadb", "1-1-1,2-2-2")
	c.Assert(err, IsNil)
	g1 = gSet.(*mariadbGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// contain
	gSet, err = ParserGTID("mariadb", "1-1-1,2-2-1")
	c.Assert(err, IsNil)
	g2 = gSet.(*mariadbGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// not contain
	gSet, err = ParserGTID("mariadb", "1-1-2,2-2-1")
	c.Assert(err, IsNil)
	g2 = gSet.(*mariadbGTIDSet)
	c.Assert(g1.Contain(g2), IsFalse)
	c.Assert(g2.Contain(g1), IsFalse)
}
