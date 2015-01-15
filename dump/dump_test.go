package dump

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql/client"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"os"
	"testing"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")

var execution = flag.String("exec", "mysqldump", "mysqldump execution path")

func Test(t *testing.T) {
	TestingT(t)
}

type schemaTestSuite struct {
	conn *client.Conn
	d    *Dumper
}

var _ = Suite(&schemaTestSuite{})

func (s *schemaTestSuite) SetUpSuite(c *C) {
	var err error
	s.conn, err = client.Connect(fmt.Sprintf("%s:3306", *host), "root", "", "")
	c.Assert(err, IsNil)

	s.d, err = NewDumper(*execution, fmt.Sprintf("%s:3306", *host), "root", "")
	c.Assert(err, IsNil)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test1")
	c.Assert(err, IsNil)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test2")
	c.Assert(err, IsNil)

	str := `CREATE TABLE IF NOT EXISTS test%d.t%d (
			id int AUTO_INCREMENT,
			name varchar(256),
			PRIMARY KEY(id)
			) ENGINE=INNODB`
	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	c.Assert(err, IsNil)

	str = `INSERT INTO test%d.t%d (name) VALUES ("a"), ("b")`

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	c.Assert(err, IsNil)
}

func (s *schemaTestSuite) TearDownSuite(c *C) {
	if s.conn != nil {
		_, err := s.conn.Execute("DROP DATABASE IF EXISTS test1")
		c.Assert(err, IsNil)

		_, err = s.conn.Execute("DROP DATABASE IF EXISTS test2")
		c.Assert(err, IsNil)

		s.conn.Close()
	}
}

func (s *schemaTestSuite) TestDump(c *C) {
	err := s.d.Dump(ioutil.Discard)
	c.Assert(err, IsNil)

	s.d.AddDatabase("test1", "test2")

	s.d.AddIgnoreTables("test1", "t2")

	err = s.d.Dump(ioutil.Discard)
	c.Assert(err, IsNil)

	s.d.AddTable("test1", "t1")

	err = s.d.Dump(os.Stdout)
	c.Assert(err, IsNil)
}
