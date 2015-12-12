package river

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`

	ESAddr string `toml:"es_addr"`

	StatAddr string `toml:"stat_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec string `toml:"mysqldump"`

	Sources []SourceConfig `toml:"source"`

	Rules []*Rule `toml:"rule"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
