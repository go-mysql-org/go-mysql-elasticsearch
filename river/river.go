package river

import (
	"fmt"
	"github.com/siddontang/go-mysql-elasticsearch/dump"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
	"github.com/siddontang/go/sync2"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// In Elasticsearch, river is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	m *MasterInfo

	rules map[string]*Rule

	quit chan struct{}
	wg   sync.WaitGroup

	dumper *dump.Dumper
	syncer *replication.BinlogSyncer
	es     *elastic.Client

	parser *parseHandler

	ev chan interface{}

	bulkSize sync2.AtomicInt64
}

func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c

	r.quit = make(chan struct{})

	r.rules = make(map[string]*Rule)

	r.parser = &parseHandler{r: r}

	r.ev = make(chan interface{}, 2048)

	os.MkdirAll(c.DataDir, 0755)

	conn, err := client.Connect(r.c.MyAddr, r.c.MyUser, r.c.MyPassword, "")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err = r.checkBinlogFormat(conn); err != nil {
		return nil, err
	}

	if err = r.prepareRule(conn); err != nil {
		return nil, err
	}

	if r.m, err = loadMasterInfo(r.masterInfoPath()); err != nil {
		return nil, err
	} else if len(r.m.Addr) != 0 && r.m.Addr != r.c.MyAddr {
		log.Infof("MySQL addr %s in old master.info, but new %s, reset", r.m.Addr, r.c.MyAddr)
		// may use another MySQL, reset
		r.m = &MasterInfo{}
	}

	r.m.Addr = r.c.MyAddr

	if r.dumper, err = dump.NewDumper(r.c.DumpExec, r.c.MyAddr, r.c.MyUser, r.c.MyPassword); err != nil {
		if err != exec.ErrNotFound {
			return nil, err
		}
		//no mysqldump, use binlog only
		r.dumper = nil
	}

	r.es = elastic.NewClient(r.c.ESAddr)

	if err = r.prepareSyncer(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return fmt.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

func (r *River) prepareRule(c *client.Conn) error {
	// first, check sources
	for _, s := range r.c.Sources {
		for _, table := range s.Tables {
			if regexp.QuoteMeta(table) != table {
				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE 
					table_name RLIKE "%s" AND table_schema = "%s";`, table, s.Schema)

				res, err := c.Execute(sql)
				if err != nil {
					return err
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return err
					}
				}
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return err
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return fmt.Errorf("no source data defined")
	}

	if r.c.Rules != nil {
		r.c.Rules.prepare()

		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if regexp.QuoteMeta(rule.Table) != rule.Table {
				tableExp := regexp.MustCompile(rule.Table)

				for k, rr := range r.rules {
					if tableExp.MatchString(rr.Table) && rule.Schema == rr.Schema {
						if _, ok := r.rules[k]; !ok {
							return fmt.Errorf("rule %s, %s not defined in source", rr.Schema, rr.Table)
						}

						// replace regexp table name to specific table name
						r.rules[k].Table = rr.Table
						r.rules[k].Index = rule.Index
						r.rules[k].Type = rule.Type
						r.rules[k].FieldMapping = rule.FieldMapping
					}
				}

			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return fmt.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				r.rules[key] = rule
			}
		}

	}

	for _, rule := range r.rules {
		if err := rule.fetchTableInfo(c); err != nil {
			return err
		}

		// table must have a PK for one column, multi columns may be supported later.

		if len(rule.TableInfo.PKColumns) != 1 {
			return fmt.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
		}
	}

	return nil
}

func (r *River) checkBinlogFormat(c *client.Conn) error {
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return err
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return fmt.Errorf("binlog must ROW format, but %s now", f)
	}

	return nil
}

func (r *River) prepareSyncer() error {
	r.syncer = replication.NewBinlogSyncer(r.c.ServerID, r.c.Flavor)

	seps := strings.Split(r.c.MyAddr, ":")
	if len(seps) != 2 {
		return fmt.Errorf("invalid mysql addr format %s, must host:port", r.c.MyAddr)
	}

	port, err := strconv.ParseUint(seps[1], 10, 16)
	if err != nil {
		return err
	}

	if err = r.syncer.RegisterSlave(seps[0], uint16(port), r.c.MyUser, r.c.MyPassword); err != nil {
		return err
	}
	return nil
}

func (r *River) masterInfoPath() string {
	return path.Join(r.c.DataDir, "master.info")
}

func ruleKey(schema string, table string) string {
	return fmt.Sprintf("%s:%s", schema, table)
}

func (r *River) Run() error {
	r.wg.Add(2)
	go r.syncLoop()

	defer r.wg.Done()

	// first check needing dump?
	if err := r.tryDump(); err != nil {
		log.Fatalf("dump mysql error %v", err)
		return err
	}

	if err := r.syncBinlog(); err != nil {
		if !r.closed() || err != mysql.ErrBadConn {
			log.Fatalf("sync binlog error %v", err)
		}
		return err
	}

	return nil
}

func (r *River) Close() {
	log.Infof("closing river")
	close(r.quit)

	r.syncer.Close()

	r.wg.Wait()

	r.m.Close()
}

func (r *River) closed() bool {
	select {
	case <-r.quit:
		return true
	default:
		return false
	}
}
