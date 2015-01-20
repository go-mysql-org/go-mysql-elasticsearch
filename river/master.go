package river

import (
	"bytes"
	"github.com/BurntSushi/toml"
	"io"
	"os"
)

type MasterInfo struct {
	Addr     string `toml:"addr"`
	Name     string `toml:"bin_name"`
	Position uint32 `toml:"bin_pos"`

	f *os.File
}

func loadMasterInfo(name string) (*MasterInfo, error) {
	var m MasterInfo

	var err error
	if m.f, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	st, _ := m.f.Stat()
	if st.Size() > 0 {
		_, err = toml.DecodeReader(m.f, &m)
	}

	return &m, err
}

func (m *MasterInfo) Save() error {
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	e.Encode(m)

	m.f.Truncate(0)
	if n, err := m.f.Write(buf.Bytes()); err != nil {
		return err
	} else if n != buf.Len() {
		return io.ErrShortWrite
	}
	return nil
}

func (m *MasterInfo) Update(name string, pos uint32) {
	m.Name = name
	m.Position = pos
}

func (m *MasterInfo) Close() {
	if m.f != nil {
		m.Save()

		m.f.Close()
		m.f = nil
	}
}
