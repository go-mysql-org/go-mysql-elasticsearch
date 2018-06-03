package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/birkirb/loggers.v1/log"
)

type elasticsearchMasterInfo struct {
	sync.RWMutex

	es *elastic.Client

	Name    string
	Pos     uint32
	index   string
	docType string
	id      string
}

func loadElasticsearchMasterInfo(dataPath string) (masterInfo, error) {
	var m elasticsearchMasterInfo

	if !strings.HasPrefix(dataPath, "es:") {
		return &m, errors.Errorf("error elasticsearch prefix: %s", dataPath)
	}
	esURL, err := url.Parse(dataPath[3:])
	if err != nil {
		return &m, err
	}

	cfg := new(elastic.ClientConfig)
	cfg.Addr = esURL.Host
	if esURL.User != nil {
		cfg.User = esURL.User.Username()
		cfg.Password, _ = esURL.User.Password()
	}
	cfg.Https = esURL.Scheme == "https"
	m.es = elastic.NewClient(cfg)

	paths := strings.Split(esURL.Path, "/")
	m.index = paths[1]
	m.docType = paths[2]

	m.id = "1"
	id := esURL.Query().Get("id")
	if id != "" {
		m.id = id
	}

	err = m.loadMasterInfo()
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (m *elasticsearchMasterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos
	doc := map[string]interface{}{
		"name": m.Name,
		"pos":  m.Pos,
	}
	err := m.es.Update(m.index, m.docType, m.id, doc)
	if err != nil {
		log.Errorf("ES MasterInfo save error: %s", err)
		return err
	}
	return nil
}

func (m *elasticsearchMasterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

func (m *elasticsearchMasterInfo) Close() error {
	pos := m.Position()
	return m.Save(pos)
}

func (m *elasticsearchMasterInfo) loadMasterInfo() error {
	mapping, err := m.es.GetMapping(m.index, m.docType)
	if err != nil || mapping.Code == http.StatusNotFound {
		err = m.createMapping()
	}

	info, err := m.es.Get(m.index, m.docType, m.id)
	if err == nil && (info.Code == http.StatusOK || info.Code == http.StatusNotFound) {
		item := info.ResponseItem
		if item.Found {
			source := item.Source
			m.Name = source["name"].(string)
			m.Pos = uint32(source["pos"].(float64))
		}
		return nil
	}
	return errors.Wrap(err, errors.New("loadMasterInfo error"))
}

func (m *elasticsearchMasterInfo) createMapping() error {
	version, _ := m.getEsVersion()
	nameType := "keyword"
	if version < 5 {
		nameType = "string"
	}
	mapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type": nameType,
			},
			"pos": map[string]interface{}{
				"type": "long",
			},
		},
	}

	return m.es.CreateMapping(m.index, m.docType, mapping)
}

func (m *elasticsearchMasterInfo) getEsVersion() (int64, error) {
	reqURL := fmt.Sprintf("%s://%s/", m.es.Protocol, m.es.Addr)
	resp, err := m.es.DoRequest("GET", reqURL, bytes.NewBuffer(nil))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var esinfo EsinfoResponse
	err = json.NewDecoder(resp.Body).Decode(&esinfo)

	version := esinfo.Version.Number
	if version == "" {
		return 0, errors.Errorf("unknow version")
	}
	v := regexp.MustCompile("^\\d+").FindString(version)
	mainVersion, err := strconv.ParseInt(v, 10, 32)
	if err == nil {
		return mainVersion, nil
	}
	return 0, errors.Errorf("unknow version")
}

type EsinfoResponse struct {
	Name        string            `json:"name"`
	ClusterName string            `json:"cluster_name"`
	Version     EsVersionResponse `json:"version"`
}

type EsVersionResponse struct {
	Number string
}
