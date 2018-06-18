package elastic

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/juju/errors"
)

// Client is the client to communicate with ES.
// Although there are many Elasticsearch clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	Protocol string
	Addr     string
	User     string
	Password string

	c *http.Client
}

// ClientConfig is the configuration for the client.
type ClientConfig struct {
	HTTPS    bool
	Addr     string
	User     string
	Password string
}

// NewClient creates the Cient with configuration.
func NewClient(conf *ClientConfig) *Client {
	c := new(Client)

	c.Addr = conf.Addr
	c.User = conf.User
	c.Password = conf.Password

	if conf.HTTPS {
		c.Protocol = "https"
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		c.c = &http.Client{Transport: tr}
	} else {
		c.Protocol = "http"
		c.c = &http.Client{}
	}

	return c
}

// ResponseItem is the ES item in the response.
type ResponseItem struct {
	ID      string                 `json:"_id"`
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	Version int                    `json:"_version"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
}

// Response is the ES response
type Response struct {
	Code int
	ResponseItem
}

// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionIndex  = "index"
)

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action   string
	Index    string
	Type     string
	ID       string
	Parent   string
	Pipeline string

	Data map[string]interface{}
}

func (r *BulkRequest) bulk(buf *bytes.Buffer) error {
	meta := make(map[string]map[string]string)
	metaData := make(map[string]string)
	if len(r.Index) > 0 {
		metaData["_index"] = r.Index
	}
	if len(r.Type) > 0 {
		metaData["_type"] = r.Type
	}

	if len(r.ID) > 0 {
		metaData["_id"] = r.ID
	}
	if len(r.Parent) > 0 {
		metaData["_parent"] = r.Parent
	}
	if len(r.Pipeline) > 0 {
		metaData["pipeline"] = r.Pipeline
	}

	meta[r.Action] = metaData

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	buf.Write(data)
	buf.WriteByte('\n')

	switch r.Action {
	case ActionDelete:
		//nothing to do
	case ActionUpdate:
		doc := map[string]interface{}{
			"doc": r.Data,
		}
		data, err = json.Marshal(doc)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	default:
		//for create and index
		data, err = json.Marshal(r.Data)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	}

	return nil
}

// BulkResponse is the response for the bulk request.
type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

// BulkResponseItem is the item in the bulk response.
type BulkResponseItem struct {
	Index   string          `json:"_index"`
	Type    string          `json:"_type"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Status  int             `json:"status"`
	Error   json.RawMessage `json:"error"`
	Found   bool            `json:"found"`
}

// MappingResponse is the response for the mapping request.
type MappingResponse struct {
	Code    int
	Mapping Mapping
}

// Mapping represents ES mapping.
type Mapping map[string]struct {
	Mappings map[string]struct {
		Properties map[string]struct {
			Type   string      `json:"type"`
			Fields interface{} `json:"fields"`
		} `json:"properties"`
	} `json:"mappings"`
}

// DoRequest sends a request with body to ES.
func (c *Client) DoRequest(method string, url string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(c.User) > 0 && len(c.Password) > 0 {
		req.SetBasicAuth(c.User, c.Password)
	}
	resp, err := c.c.Do(req)

	return resp, err
}

// Do sends the request with body to ES.
func (c *Client) Do(method string, url string, body map[string]interface{}) (*Response, error) {
	bodyData, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bytes.NewBuffer(bodyData)
	if body == nil {
		buf = bytes.NewBuffer(nil)
	}

	resp, err := c.DoRequest(method, url, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(Response)
	ret.Code = resp.StatusCode

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret.ResponseItem)
	}

	return ret, errors.Trace(err)
}

// DoBulk sends the bulk request to the ES.
func (c *Client) DoBulk(url string, items []*BulkRequest) (*BulkResponse, error) {
	var buf bytes.Buffer

	for _, item := range items {
		if err := item.bulk(&buf); err != nil {
			return nil, errors.Trace(err)
		}
	}

	resp, err := c.DoRequest("POST", url, &buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(BulkResponse)
	ret.Code = resp.StatusCode

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret)
	}

	return ret, errors.Trace(err)
}

// CreateMapping creates a ES mapping.
func (c *Client) CreateMapping(index string, docType string, mapping map[string]interface{}) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	// if index doesn't exist, will get 404 not found, create index first
	if r.Code == http.StatusNotFound {
		_, err = c.Do("PUT", reqURL, nil)

		if err != nil {
			return errors.Trace(err)
		}
	} else if r.Code != http.StatusOK {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}

	reqURL = fmt.Sprintf("%s://%s/%s/%s/_mapping", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))

	_, err = c.Do("POST", reqURL, mapping)
	return errors.Trace(err)
}

// GetMapping gets the mapping.
func (c *Client) GetMapping(index string, docType string) (*MappingResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/%s/_mapping", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))
	buf := bytes.NewBuffer(nil)
	resp, err := c.DoRequest("GET", reqURL, buf)

	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := new(MappingResponse)
	err = json.Unmarshal(data, &ret.Mapping)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret.Code = resp.StatusCode
	return ret, errors.Trace(err)
}

// DeleteIndex deletes the index.
func (c *Client) DeleteIndex(index string) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Get gets the item by id.
func (c *Client) Get(index string, docType string, id string) (*Response, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	return c.Do("GET", reqURL, nil)
}

// Update creates or updates the data
func (c *Client) Update(index string, docType string, id string, data map[string]interface{}) error {
	reqURL := fmt.Sprintf("%s://%s/%s/%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("PUT", reqURL, data)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusCreated {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Exists checks whether id exists or not.
func (c *Client) Exists(index string, docType string, id string) (bool, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return false, err
	}

	return r.Code == http.StatusOK, nil
}

// Delete deletes the item by id.
func (c *Client) Delete(index string, docType string, id string) error {
	reqURL := fmt.Sprintf("%s://%s/%s/%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Bulk sends the bulk request.
// only support parent in 'Bulk' related apis
func (c *Client) Bulk(items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/_bulk", c.Protocol, c.Addr)

	return c.DoBulk(reqURL, items)
}

// IndexBulk sends the bulk request for index.
func (c *Client) IndexBulk(index string, items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_bulk", c.Protocol, c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqURL, items)
}

// IndexTypeBulk sends the bulk request for index and doc type.
func (c *Client) IndexTypeBulk(index string, docType string, items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/%s/_bulk", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))

	return c.DoBulk(reqURL, items)
}
