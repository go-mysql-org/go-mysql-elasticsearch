package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/juju/errors"
)

// Although there are many Elasticsearch clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	Addr string

	c *http.Client
}

func NewClient(addr string) *Client {
	c := new(Client)

	c.Addr = addr

	c.c = &http.Client{}

	return c
}

type ResponseItem struct {
	ID      string                 `json:"_id"`
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	Version int                    `json:"_version"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
}

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

type BulkRequest struct {
	Action string
	Index  string
	Type   string
	ID     string
	Parent string

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

type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

type BulkResponseItem struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Status  int    `json:"status"`
	Error   string `json:"error"`
	Found   bool   `json:"found"`
}

func (c *Client) Do(method string, url string, body map[string]interface{}) (*Response, error) {
	bodyData, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bytes.NewBuffer(bodyData)

	req, err := http.NewRequest(method, url, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := new(Response)
	ret.Code = resp.StatusCode

	if resp.ContentLength > 0 {
		d := json.NewDecoder(resp.Body)
		err = d.Decode(&ret.ResponseItem)
	}

	resp.Body.Close()

	return ret, err
}

func (c *Client) DoBulk(url string, items []*BulkRequest) (*BulkResponse, error) {
	var buf bytes.Buffer

	for _, item := range items {
		if err := item.bulk(&buf); err != nil {
			return nil, errors.Trace(err)
		}
	}

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := new(BulkResponse)
	ret.Code = resp.StatusCode

	if resp.ContentLength > 0 {
		d := json.NewDecoder(resp.Body)
		err = d.Decode(&ret)
	}

	resp.Body.Close()

	return ret, err
}

func (c *Client) CreateMapping(index string, docType string, mapping map[string]interface{}) error {
	reqUrl := fmt.Sprintf("http://%s/%s", c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("HEAD", reqUrl, nil)
	if err != nil {
		return errors.Trace(err)
	}

	// index doesn't exist, create index first
	if r.Code != http.StatusOK {
		_, err = c.Do("POST", reqUrl, nil)

		if err != nil {
			return errors.Trace(err)
		}
	}

	reqUrl = fmt.Sprintf("http://%s/%s/%s/_mapping", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))

	_, err = c.Do("POST", reqUrl, mapping)
	return errors.Trace(err)
}

func (c *Client) DeleteIndex(index string) error {
	reqUrl := fmt.Sprintf("http://%s/%s", c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("DELETE", reqUrl, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	} else {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}
}

func (c *Client) Get(index string, docType string, id string) (*Response, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	return c.Do("GET", reqUrl, nil)
}

// Can use Update to create or update the data
func (c *Client) Update(index string, docType string, id string, data map[string]interface{}) error {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("PUT", reqUrl, data)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusCreated {
		return nil
	} else {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}
}

func (c *Client) Exists(index string, docType string, id string) (bool, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("HEAD", reqUrl, nil)
	if err != nil {
		return false, err
	}

	return r.Code == http.StatusOK, nil
}

func (c *Client) Delete(index string, docType string, id string) error {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id))

	r, err := c.Do("DELETE", reqUrl, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	} else {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}
}

// only support parent in 'Bulk' related apis
func (c *Client) Bulk(items []*BulkRequest) (*BulkResponse, error) {
	reqUrl := fmt.Sprintf("http://%s/_bulk", c.Addr)

	return c.DoBulk(reqUrl, items)
}

func (c *Client) IndexBulk(index string, items []*BulkRequest) (*BulkResponse, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/_bulk", c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqUrl, items)
}

func (c *Client) IndexTypeBulk(index string, docType string, items []*BulkRequest) (*BulkResponse, error) {
	reqUrl := fmt.Sprintf("http://%s/%s/%s/_bulk", c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType))

	return c.DoBulk(reqUrl, items)
}
