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

	// for MySQL binlog row image
	rowImage string

	dumpDoneCh chan struct{}

	connLock sync.Mutex
	conn     *client.Conn

	st *stat
}

func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c

	r.quit = make(chan struct{})

	r.rules = make(map[string]*Rule)

	r.dumpDoneCh = make(chan struct{})

	r.parser = &parseHandler{r: r}

	r.ev = make(chan interface{}, 2048)

	os.MkdirAll(c.DataDir, 0755)

	var err error

	r.conn, err = client.Connect(r.c.MyAddr, r.c.MyUser, r.c.MyPassword, "")
	if err != nil {
		return nil, err
	}

	if err = r.checkBinlogFormat(r.conn); err != nil {
		return nil, err
	}

	if err = r.prepareRule(r.conn); err != nil {
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

	r.st = &stat{r: r}
	go r.st.Run(r.c.StatAddr)

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

func (r *River) parseSource(c *client.Conn) (map[string][]string, error) {
	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, fmt.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, fmt.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE 
                    table_name RLIKE "%s" AND table_schema = "%s";`, table, s.Schema)

				res, err := c.Execute(sql)
				if err != nil {
					return nil, err
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, err
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return nil, fmt.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule(c *client.Conn) error {
	wildtables, err := r.parseSource(c)
	if err != nil {
		return err
	}

	if r.c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.Schema) == 0 {
				return fmt.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return fmt.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}

				if len(rule.Index) == 0 {
					return fmt.Errorf("wildcard table rule %s.%s must have a index, can not empty", rule.Schema, rule.Table)
				}

				rule.prepare()

				for _, table := range tables {
					rr := r.rules[ruleKey(rule.Schema, table)]
					rr.Index = rule.Index
					rr.Type = rule.Type
					rr.FieldMapping = rule.FieldMapping
				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return fmt.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.prepare()
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

	// default binlog row image, mariadb use full too
	r.rowImage = replication.BINLOG_ROW_IMAGE_FULL
	// need to check MySQL binlog row image? full, minimal or noblob?
	if r.c.Flavor == mysql.MySQLFlavor {
		if res, err = c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`); err != nil {
			return err
		}

		r.rowImage, _ = res.GetString(0, 1)
		log.Infof("MySQL use binlog row %s image", r.rowImage)
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

	close(r.dumpDoneCh)

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

	r.connLock.Lock()
	r.conn.Close()
	r.connLock.Unlock()

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

func (r *River) executeSql(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	r.connLock.Lock()
	defer r.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if r.conn == nil {
			r.conn, err = client.Connect(r.c.MyAddr, r.c.MyUser, r.c.MyPassword, "")
			if err != nil {
				return nil, err
			}
		}

		rr, err = r.conn.Execute(cmd, args...)
		if err != nil && err != mysql.ErrBadConn {
			return
		} else if err == mysql.ErrBadConn {
			r.conn = nil
			continue
		} else {
			return
		}
	}
	return
}
