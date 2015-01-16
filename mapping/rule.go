package mapping

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
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
	// If you want to custom the field mapping, set it below.
	FieldMapping map[string]string `toml:"mapping"`
}

type Rules struct {
	Rules []*Rule `toml:"rule"`
}

func LoadRules(data []byte) (*Rules, error) {
	var rules Rules

	_, err := toml.Decode(string(data), &rules)
	if err != nil {
		return nil, err
	}

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
	return nil
}
