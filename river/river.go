package river

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/siddontang/go-mysql/canal"

	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go/log"
)

// In Elasticsearch, river is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	canal *canal.Canal

	rules map[string]*Rule

	quit chan struct{}
	wg   sync.WaitGroup

	es *elastic.Client

	st *stat
}

func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c

	r.quit = make(chan struct{})

	r.rules = make(map[string]*Rule)

	var err error
	if err = r.newCanal(); err != nil {
		return nil, err
	}

	if err = r.prepareRule(); err != nil {
		return nil, err
	}

	if err = r.prepareCanal(); err != nil {
		return nil, err
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, err
	}

	r.es = elastic.NewClient(r.c.ESAddr)

	r.st = &stat{r: r}
	go r.st.Run(r.c.StatAddr)

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Flavor = r.c.Flavor
	cfg.DataDir = r.c.DataDir

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return err
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		db = rule.Schema
		dbs[rule.Schema] = struct{}{}
		tables = append(tables, rule.Table)
	}

	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key, _ := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}

	r.canal.RegRowsEventHandler(&rowsEventHandler{r})

	return nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return fmt.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

func (r *River) parseSource() (map[string][]string, error) {
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

				res, err := r.canal.Execute(sql)
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

func (r *River) prepareRule() error {
	wildtables, err := r.parseSource()
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
		if rule.TableInfo, err = r.canal.GetTable(rule.Schema, rule.Table); err != nil {
			return err
		}

		// table must have a PK for one column, multi columns may be supported later.

		if len(rule.TableInfo.PKColumns) != 1 {
			return fmt.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
		}
	}

	return nil
}

func ruleKey(schema string, table string) string {
	return fmt.Sprintf("%s:%s", schema, table)
}

func (r *River) Run() error {
	if err := r.canal.Start(); err != nil {
		log.Errorf("start canal err %v", err)
		return err
	}

	return nil
}

func (r *River) Close() {
	log.Infof("closing river")
	close(r.quit)

	r.canal.Close()

	r.wg.Wait()
}
