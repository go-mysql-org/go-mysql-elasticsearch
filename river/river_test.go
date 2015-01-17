package river

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {

}

func (s *riverTestSuite) TearDownSuite(c *C) {

}

func (s *riverTestSuite) TestConfig(c *C) {
	str := `
my_addr = "127.0.0.1:3306"
my_user = "root"
my_pass = ""

es_addr = "127.0.0.1:9200"

data_dir = "./var"

[[source]]
schema = "test"
tables = "es_test"

[[rule]]
schema = "test"
table = "es_test"
index = "es_test"
type = "es_test"

    [rule.mapping]
    name = "es_name"
`

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Rules, HasLen, 1)
}
