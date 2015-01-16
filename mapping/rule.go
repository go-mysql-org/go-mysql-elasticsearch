package mapping

import (
	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql/schema"
	"io/ioutil"
	"sort"
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
	FieldMapping map[string]string `toml:"mapping"`

	// MySQL table information
	TableInfo *schema.Table
}

type RuleSlice []*Rule

func (rs RuleSlice) Len() int {
	return len(rs)
}

func (rs RuleSlice) Less(i, j int) bool {
	if rs[i].Schema < rs[j].Schema {
		return true
	} else if rs[i].Schema > rs[j].Schema {
		return false
	} else {
		return rs[i].Table < rs[j].Table
	}
}

func (rs RuleSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

type Rules struct {
	Rules RuleSlice `toml:"rule"`
}

func LoadRules(data []byte) (*Rules, error) {
	var rules Rules

	_, err := toml.Decode(string(data), &rules)
	if err != nil {
		return nil, err
	}

	sort.Sort(rules.Rules)

	for _, rule := range rules.Rules {
		if len(rule.Index) == 0 {
			rule.Index = rule.Table
		}

		if len(rule.Type) == 0 {
			rule.Type = rule.Index
		}
	}

	// todo, check invalid config

	return &rules, nil
}

func LoadRulesWithFile(file string) (*Rules, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return LoadRules(data)
}

func (r *Rules) GetRule(schema string, table string) *Rule {
	i := sort.Search(len(r.Rules), func(i int) bool {
		if r.Rules[i].Schema == schema {
			return r.Rules[i].Table >= table
		} else {
			return r.Rules[i].Schema > schema
		}
	})
	if i < len(r.Rules) && r.Rules[i].Schema == schema && r.Rules[i].Table == table {
		return r.Rules[i]
	} else {
		return nil
	}
}
