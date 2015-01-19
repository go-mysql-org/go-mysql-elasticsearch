package river

import (
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql-elasticsearch/mapping"
	"github.com/siddontang/go/ioutil2"
	"io/ioutil"
)

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_password"`

	ESAddr string `toml:"es_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec string `toml:"mysqldump"`

	Sources []SourceConfig `toml:"source"`

	Rules mapping.Rules `toml:"rule"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

type MasterInfo struct {
	Addr     string `toml:"addr"`
	Name     string `toml:"bin_name"`
	Position uint64 `toml:"bin_pos"`
}

func LoadMasterInfo(name string) (*MasterInfo, error) {
	var m MasterInfo
	if !ioutil2.FileExists(name) {
		return &m, nil
	}

	_, err := toml.DecodeFile(name, &m)
	return &m, err
}

func (m *MasterInfo) Save(name string) error {
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	e.Encode(m)

	return ioutil2.WriteFileAtomic(name, buf.Bytes(), 0644)
}

func (m *MasterInfo) Update(name string, pos uint64) {
	m.Name = name
	m.Position = pos
}
