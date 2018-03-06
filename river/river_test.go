package river

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
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
            mylist VARCHAR(256),
            mydate INT(10),
            tenum ENUM("e1", "e2", "e3"),
            tset SET("a", "b", "c"),
            tbit BIT(1) default 1,
            tdatetime DATETIME DEFAULT NULL,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	schema_json := `
	CREATE TABLE IF NOT EXISTS %s (
	    id INT,
	    info JSON,
	    PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(c, "DROP TABLE IF EXISTS test_river")
	s.testExecute(c, "DROP TABLE IF EXISTS test_for_id")
	s.testExecute(c, "DROP TABLE IF EXISTS test_for_json")
	s.testExecute(c, fmt.Sprintf(schema, "test_river"))
	s.testExecute(c, fmt.Sprintf(schema, "test_for_id"))
	s.testExecute(c, fmt.Sprintf(schema_json, "test_for_json"))

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		s.testExecute(c, fmt.Sprintf(schema, table))
	}

	cfg := new(Config)
	cfg.MyAddr = *my_addr
	cfg.MyUser = "root"
	cfg.MyPassword = ""
	cfg.MyCharset = "utf8"
	cfg.ESAddr = *es_addr

	cfg.ServerID = 1001
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river"
	cfg.DumpExec = "mysqldump"

	cfg.StatAddr = "127.0.0.1:12800"
	cfg.BulkSize = 1
	cfg.FlushBulkTime = TomlDuration{3 * time.Millisecond}

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: "test", Tables: []string{"test_river", "test_river_[0-9]{4}", "test_for_id", "test_for_json"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: "test",
			Table:        "test_river",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list", "mydate": ",date"},
		},

		&Rule{Schema: "test",
			Table:        "test_for_id",
			Index:        "river",
			Type:         "river",
			ID:           []string{"id", "title"},
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list", "mydate": ",date"},
		},

		&Rule{Schema: "test",
			Table:        "test_river_[0-9]{4}",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list", "mydate": ",date"},
		},

		&Rule{Schema: "test",
			Table: "test_for_json",
			Index: "river",
			Type:  "river",
		},
	}

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
my_charset = "utf8"
es_addr = "127.0.0.1:9200"
es_user = ""
es_pass = ""
data_dir = "./var"

[[source]]
schema = "test"

tables = ["test_river", "test_river_[0-9]{4}", "test_for_id", "test_for_json"]

[[rule]]
schema = "test"
table = "test_river"
index = "river"
type = "river"
parent = "pid"

    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"
    mydate = ",date"


[[rule]]
schema = "test"
table = "test_for_id"
index = "river"
type = "river"
parent = "pid"
id = ["id", "title"]
    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"
    mydate = ",date"


[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"

    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"
    mydate = ",date"

[[rule]]
schema = "test"
table = "test_for_json"
index = "river"
type = "river"
`

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Sources[0].Tables, HasLen, 4)
	c.Assert(cfg.Rules, HasLen, 4)
}

func (s *riverTestSuite) testExecute(c *C, query string, args ...interface{}) {
	_, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 2, "second", "hello mysql 2", "e2", "b,c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 3, "third", "hello elaticsearch 3", "e3", "c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset, tbit) VALUES (?, ?, ?, ?, ?, ?)", 4, "fouth", "hello go-mysql-elasticserach 4", "e1", "a,b,c", 0)
	s.testExecute(c, "INSERT INTO test_for_id (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.testExecute(c, "INSERT INTO test_for_json (id, info) VALUES (?, ?)", 9200, "{\"first\": \"a\", \"second\": \"b\"}")

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("INSERT INTO %s (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", table), 5+i, "abc", "hello", "e1", "a,b,c")
	}

	datetime := time.Now().Format(mysql.TimeFormat)
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset, tdatetime, mydate) VALUES (?, ?, ?, ?, ?, ?, ?)", 16, "test datetime", "hello go 16", "e1", "a,b", datetime, 1458131094)
}

func (s *riverTestSuite) testElasticGet(c *C, id string) *elastic.Response {
	index := "river"
	docType := "river"

	r, err := s.r.es.Get(index, docType, id)
	c.Assert(err, IsNil)

	return r
}

func (s *riverTestSuite) testElasticMapping(c *C) *elastic.MappingResponse {
	index := "river"
	docType := "river"

	r, err := s.r.es.GetMapping(index, docType)
	c.Assert(err, IsNil)

	c.Assert(r.Mapping[index].Mappings[docType].Properties["tdatetime"].Type, Equals, "date")
	c.Assert(r.Mapping[index].Mappings[docType].Properties["mydate"].Type, Equals, "date")
	return r
}

func testWaitSyncDone(c *C, r *River) {
	<-r.canal.WaitDumpDone()

	err := r.canal.CatchMasterPos(10 * time.Second)
	c.Assert(err, IsNil)

	for i := 0; i < 1000; i++ {
		if len(r.syncCh) == 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	c.Fatalf("wait 1s but still have %d items to be synced", len(r.syncCh))
}

func (s *riverTestSuite) TestRiver(c *C) {
	s.testPrepareData(c)

	go func() { s.r.Run() }()

	testWaitSyncDone(c, s.r)

	var mr *elastic.MappingResponse
	mr = s.testElasticMapping(c)
	c.Assert(mr.Code, Equals, 200)

	var r *elastic.Response
	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["tenum"], Equals, "e1")
	c.Assert(r.Source["tset"], Equals, "a,b")

	r = s.testElasticGet(c, "1:first")
	c.Assert(r.Found, Equals, true)

	r = s.testElasticGet(c, "9200")
	c.Assert(r.Found, Equals, true)
	switch v := r.Source["info"].(type) {
	case map[string]interface{}:
		c.Assert(v["first"], Equals, "a")
		c.Assert(v["second"], Equals, "b")
	default:
		c.Assert(v, Equals, nil)
		c.Assert(true, Equals, false)
	}

	r = s.testElasticGet(c, "100")
	c.Assert(r.Found, Equals, false)

	for i := 0; i < 10; i++ {
		r = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["es_title"], Equals, "abc")
	}

	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	s.testExecute(c, "DELETE FROM test_river WHERE id = ?", 1)
	s.testExecute(c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	// so we can insert invalid data
	s.testExecute(c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	testWaitSyncDone(c, s.r)

	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, false)

	r = s.testElasticGet(c, "2")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["es_title"], Equals, "second 2")
	c.Assert(r.Source["tenum"], Equals, "e3")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["es_mylist"], DeepEquals, []interface{}{"a", "b", "c"})
	c.Assert(r.Source["tbit"], Equals, float64(1))

	r = s.testElasticGet(c, "4")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["tenum"], Equals, "")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["tbit"], Equals, float64(0))

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

func TestTableValidation(t *testing.T) {
	tables := []struct {
		Tables []string
		Expect bool
	}{
		{[]string{"*"}, true},
		{[]string{"table", "table2"}, true},
		{[]string{"*", "table"}, false},
	}

	for _, table := range tables {
		if isValidTables(table.Tables) != table.Expect {
			t.Errorf("Tables: %s, Expected: is %t, but: was %t", table.Tables, table.Expect, isValidTables(table.Tables))
		}
	}
}

func TestBuildTable(t *testing.T) {
	tables := []struct {
		Table  string
		Expect string
	}{
		{"*", ".*"},
		{"table2", "table2"},
	}

	for _, table := range tables {
		if buildTable(table.Table) != table.Expect {
			t.Errorf("Table: %s, Expected: is \"%s\", but: was \"%s\"", table.Table, table.Expect, buildTable(table.Table))
		}
	}
}
