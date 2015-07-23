package elastic

import (
	"flag"
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
)

var host = flag.String("host", "127.0.0.1", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

func Test(t *testing.T) {
	TestingT(t)
}

type elasticTestSuite struct {
	c *Client
}

var _ = Suite(&elasticTestSuite{})

func (s *elasticTestSuite) SetUpSuite(c *C) {
	s.c = NewClient(fmt.Sprintf("%s:%d", *host, *port))
}

func (s *elasticTestSuite) TearDownSuite(c *C) {

}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func (s *elasticTestSuite) TestSimple(c *C) {
	index := "dummy"
	docType := "blog"

	//key1 := "name"
	//key2 := "content"

	err := s.c.Update(index, docType, "1", makeTestData("abc", "hello world"))
	c.Assert(err, IsNil)

	exists, err := s.c.Exists(index, docType, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)

	r, err := s.c.Get(index, docType, "1")
	c.Assert(err, IsNil)
	c.Assert(r.ID, Equals, "1")

	err = s.c.Delete(index, docType, "1")
	c.Assert(err, IsNil)

	err = s.c.Delete(index, docType, "1")
	c.Assert(err, IsNil)

	exists, err = s.c.Exists(index, docType, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, false)

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		items[i] = req
	}

	resp, err := s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Errors, Equals, false)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.ID = id
		items[i] = req
	}

	resp, err = s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Errors, Equals, false)
}

// this requires a parent setting in _mapping
func (s *elasticTestSuite) TestParent(c *C) {
	index := "dummy"
	docType := "comment"

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		req.Parent = "1"
		items[i] = req
	}

	resp, err := s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Errors, Equals, false)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.ID = id
		items[i] = req
	}
	resp, err = s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Errors, Equals, false)
}
