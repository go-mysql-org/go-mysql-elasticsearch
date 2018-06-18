package river

import (
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// SourceConfig is the configs for source
type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

// Config is the configuration
type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`
	MyCharset  string `toml:"my_charset"`

	ESHttps    bool   `toml:"es_https"`
	ESAddr     string `toml:"es_addr"`
	ESUser     string `toml:"es_user"`
	ESPassword string `toml:"es_pass"`

	StatAddr string `toml:"stat_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec       string `toml:"mysqldump"`
	SkipMasterData bool   `toml:"skip_master_data"`

	Sources []SourceConfig `toml:"source"`

	Rules []*Rule `toml:"rule"`

	BulkSize int `toml:"bulk_size"`

	FlushBulkTime TomlDuration `toml:"flush_bulk_time"`

	SkipNoPkTable bool `toml:"skip_no_pk_table"`
}

// NewConfigWithFile creates a Config from file.
func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

// NewConfig creates a Config from data.
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

// TomlDuration supports time codec for TOML format.
type TomlDuration struct {
	time.Duration
}

// UnmarshalText implementes TOML UnmarshalText
func (d *TomlDuration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
