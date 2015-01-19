package river

import (
	"fmt"
	"github.com/siddontang/go-mysql-elasticsearch/dump"
	"github.com/siddontang/go-mysql/schema"
	"github.com/siddontang/go/log"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"
)

type parseHandler struct {
	r *River

	name string
	pos  uint64

	rule *Rule
	rows [][]interface{}
}

func (h *parseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

func (h *parseHandler) trySyncDoc(force bool) {
	if h.rule == nil {
		return
	}

	if len(h.rows) > 10 || (force && len(h.rows) > 0) {
		if err := h.r.syncDocument(h.rule, syncInsertDoc, h.rows); err != nil {
			log.Errorf("sync %d docs %v error %v", len(h.rows), h.rows, err)
		}
		h.rows = h.rows[0:0]
	}
}

func (h *parseHandler) Data(db string, table string, values []string) error {
	rule, ok := h.r.rules[ruleKey(db, table)]
	if !ok {
		// no rule, skip this data
		log.Warnf("no rule for %s.%s", db, table)
		return nil
	}

	h.trySyncDoc(rule != h.rule)

	h.rule = rule

	vs := make([]interface{}, len(values))

	for i, v := range values {
		if v == "NULL" {
			vs[i] = nil
		} else if v[0] != '\'' {
			if rule.TableInfo.Columns[i].Type == schema.TYPE_NUMBER {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					log.Errorf("paser row %v at %d error %v, skip", values, i, err)
					return dump.ErrSkip
				}
				vs[i] = n
			} else if rule.TableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					log.Errorf("paser row %v at %d error %v, skip", values, i, err)
					return dump.ErrSkip
				}
				vs[i] = f
			} else {
				log.Errorf("paser row %v error, invalid type at %d, skip", values, i)
				return dump.ErrSkip
			}
		} else {
			vs[i] = v[1 : len(v)-1]
		}
	}

	h.rows = append(h.rows, vs)

	return nil
}

func (r *River) tryDump() error {
	if len(r.m.Name) > 0 && r.m.Position > 0 {
		// we will sync with binlog name and position
		log.Infof("skip dump, use last binlog replication pos (%s, %d)", r.m.Name, r.m.Position)
		return nil
	}

	name := path.Join(r.c.DataDir, fmt.Sprintf("%s.sql", time.Now().String()))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(name)
	}()

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
		r.dumper.AddTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key, _ := range dbs {
			keys = append(keys, key)
		}

		r.dumper.AddDatabases(keys...)
	}

	r.dumper.SetErrOut(ioutil.Discard)

	log.Info("try dump MySQL")
	if err = r.dumper.Dump(f); err != nil {
		return err
	}

	log.Info("dump MySQL OK, try parse")

	f.Seek(0, 0)

	// do we need to delete the associated index in Elasticserach????
	if err = dump.Parse(f, r.parser); err != nil {
		return err
	}

	r.parser.trySyncDoc(true)

	log.Infof("parse dump MySQL data OK, start binlog replication at (%s, %d)", r.parser.name, r.parser.pos)

	// set binlog information for sync
	r.m.Addr = r.c.MyAddr
	r.m.Name = r.parser.name
	r.m.Position = r.parser.pos

	return nil
}
