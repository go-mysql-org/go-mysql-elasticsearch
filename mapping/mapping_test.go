package mapping

import (
	"github.com/BurntSushi/toml"
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type mappingTestSuite struct {
}

var _ = Suite(&mappingTestSuite{})

func (s *mappingTestSuite) SetUpSuite(c *C) {

}

func (s *mappingTestSuite) TearDownSuite(c *C) {

}

func (s *mappingTestSuite) TestRule(c *C) {
	str := `
        schema = "test"
        table = "t"
        index = "t"
        type = "t"

        [mapping]
        a = "ab"
        b = "bb"
    `

	var rule Rule

	_, err := toml.Decode(str, &rule)
	c.Assert(err, IsNil)
	c.Assert(rule.Schema, Equals, "test")
	c.Assert(rule.FieldMapping, DeepEquals, map[string]string{"a": "ab", "b": "bb"})

	var rules struct {
		Rules `toml:"rule"`
	}

	str = `
    [[rule]]
    schema = "test"
    table = "t"
    index = "t"
    type = "t"

        [rule.mapping]
        a = "ab"
        b = "bb"

    [[rule]]
    schema = "test1"
    table = "t1"
    index = "t1"
    type = "t1"

        [rule.mapping]
        a = "ab1"
        b = "bb1"

    `

	_, err = toml.Decode(str, &rules)
	c.Assert(err, IsNil)
	c.Assert(rules.Rules, HasLen, 2)
}
