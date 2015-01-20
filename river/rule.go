package river

import (
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/schema"
)

// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string `toml:"schema"`
	Table  string `toml:"table"`
	Index  string `toml:"index"`
	Type   string `toml:"type"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table
	r.Index = table
	r.Type = table
	r.FieldMapping = make(map[string]string)

	return r
}

type Rules []*Rule

func (r Rules) prepare() error {
	for _, rule := range r {
		if rule.FieldMapping == nil {
			rule.FieldMapping = make(map[string]string)
		}

		if len(rule.Index) == 0 {
			rule.Index = rule.Table
		}

		if len(rule.Type) == 0 {
			rule.Type = rule.Index
		}
	}
	return nil
}

func (r *Rule) fetchTableInfo(conn *client.Conn) error {
	var err error
	r.TableInfo, err = schema.NewTable(conn, r.Schema, r.Table)
	if err != nil {
		return err
	}

	return nil
}
