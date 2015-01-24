package river

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/client"
	. "gopkg.in/check.v1"
	"os"
	"testing"
	"time"
)

var my_addr = flag.String("my_addr", "127.0.0.1:3306", "MySQL addr")
var es_addr = flag.String("es_addr", "127.0.0.1:9200", "Elasticsearch addr")

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
	c *client.Conn
	r *River
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	var err error
	s.c, err = client.Connect(*my_addr, "root", "", "test")
	c.Assert(err, IsNil)

	s.testExecute(c, "SET SESSION binlog_format = 'ROW'")

	schema := `
        CREATE TABLE IF NOT EXISTS %s (
            id INT, 
            title VARCHAR(256),
            content VARCHAR(256),
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(c, "DROP TABLE IF EXISTS test_river")
	s.testExecute(c, fmt.Sprintf(schema, "test_river"))

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		s.testExecute(c, fmt.Sprintf(schema, table))
	}

	cfg := new(Config)
	cfg.MyAddr = *my_addr
	cfg.MyUser = "root"
	cfg.MyPassword = ""
	cfg.ESAddr = *es_addr

	cfg.ServerID = 1001
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river"
	cfg.DumpExec = "mysqldump"

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: "test", Tables: []string{"test_river", "test_river_[0-9]{4}"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: "test",
			Table:        "test_river",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title"}},

		&Rule{Schema: "test",
			Table:        "test_river_[0-9]{4}",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title"}}}

	s.r, err = NewRiver(cfg)
	c.Assert(err, IsNil)

	err = s.r.es.DeleteIndex("river")
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.c != nil {
		s.c.Close()
	}

	if s.r != nil {
		s.r.Close()
	}
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

tables = ["test_river", "test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river"
index = "river"
type = "river"
    [rule.field]
    title = "es_title"

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"
    [rule.field]
    title = "es_title"

`

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Sources[0].Tables, HasLen, 2)
	c.Assert(cfg.Rules, HasLen, 2)
}

func (s *riverTestSuite) testExecute(c *C, query string, args ...interface{}) {
	_, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.testExecute(c, "INSERT INTO test_river (id, title, content) VALUES (?, ?, ?)", 1, "first", "hello go 1")
	s.testExecute(c, "INSERT INTO test_river (id, title, content) VALUES (?, ?, ?)", 2, "second", "hello mysql 2")
	s.testExecute(c, "INSERT INTO test_river (id, title, content) VALUES (?, ?, ?)", 3, "third", "hello elaticsearch 3")
	s.testExecute(c, "INSERT INTO test_river (id, title, content) VALUES (?, ?, ?)", 4, "fouth", "hello go-mysql-elasticserach 4")

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("INSERT INTO %s (id, title, content) VALUES (?, ?, ?)", table), 5+i, "abc", "hello")
	}
}

func (s *riverTestSuite) testElasticGet(c *C, id string) *elastic.Response {
	index := "river"
	docType := "river"

	r, err := s.r.es.Get(index, docType, id)
	c.Assert(err, IsNil)

	return r
}

func (s *riverTestSuite) testWaitSyncDone(c *C) {
	for {
		time.Sleep(1 * time.Second)

		if s.r.bulkSize.Get() == 0 {
			break
		}
	}
}

func (s *riverTestSuite) TestRiver(c *C) {
	s.testPrepareData(c)

	go s.r.Run()

	<-s.r.dumpDoneCh

	var r *elastic.Response
	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, true)
	r = s.testElasticGet(c, "100")
	c.Assert(r.Found, Equals, false)

	for i := 0; i < 10; i++ {
		r = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["es_title"], Equals, "abc")
	}

	s.testExecute(c, "UPDATE test_river SET title = ? WHERE id = ?", "second 2", 2)
	s.testExecute(c, "DELETE FROM test_river WHERE id = ?", 1)
	s.testExecute(c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	s.testWaitSyncDone(c)

	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, false)

	r = s.testElasticGet(c, "2")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["es_title"], Equals, "second 2")

	r = s.testElasticGet(c, "3")
	c.Assert(r.Found, Equals, false)

	r = s.testElasticGet(c, "30")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["es_title"], Equals, "second 30")

	for i := 0; i < 10; i++ {
		r = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["es_title"], Equals, "hello")
	}
}
