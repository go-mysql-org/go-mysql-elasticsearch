package river

import (
	"fmt"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

func (r *River) syncDocument(rule *Rule, dtype int, values []interface{}, afterValues []interface{}) error {
	if len(rule.TableInfo.Columns) != len(values) {
		return fmt.Errorf("invalid table format for %s, column number is %d, but real data is %d",
			rule.Table, len(rule.TableInfo.Columns), len(values))
	}

	id, err := r.getDocID(rule, values)
	if err != nil {
		return err
	}

	reqs := make([]*elastic.BulkRequest, 0, 1)

	req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id}

	if dtype == syncDeleteDoc {
		req.Action = elastic.ActionDelete
	} else if dtype == syncUpdateDoc {
		afterId, err := r.getDocID(rule, afterValues)
		if err != nil {
			return err
		}

		if id != afterId {
			// PK has been changed in update, delete old id first
			req.Action = elastic.ActionDelete
			reqs = append(reqs, req)

			req = &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: afterId}
		}
		makeReqData(req, rule, afterValues)
	} else {
		makeReqData(req, rule, values)
	}

	reqs = append(reqs, req)

	// todo, use bulk, now, send one by one
	_, err = r.es.Bulk(reqs)

	return err
}

func makeReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = elastic.ActionIndex

	for i, c := range rule.TableInfo.Columns {
		if values[i] == nil {
			// need to discard nil value ?????
			continue
		}

		if name, ok := rule.FieldMapping[c.Name]; ok {
			// has custom field mapping
			req.Data[name] = values[i]
		} else {
			req.Data[c.Name] = values[i]
		}
	}
}

func (r *River) getDocID(rule *Rule, values []interface{}) (string, error) {
	// now only support one column PK
	id := values[rule.TableInfo.PKColumns[0]]

	if id == nil {
		return "", fmt.Errorf("%s PK is nil for data %v", rule.Table, values)
	}

	return fmt.Sprintf("%v", id), nil
}
