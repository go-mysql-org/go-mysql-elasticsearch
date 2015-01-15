package dump

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql/client"
	. "gopkg.in/check.v1"
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
}

func (s *schemaTestSuite) TearDownSuite(c *C) {
}

func (s *schemaTestSuite) TestSimpleDump(c *C) {
	err := s.d.Dump(os.Stdout)
	c.Assert(err, IsNil)
}
