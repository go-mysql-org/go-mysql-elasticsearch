package river

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
)

type rowsEventHandler struct {
	r *River
}

func (h *rowsEventHandler) Do(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var reqs []*elastic.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		return errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		return errors.Errorf("make %s ES request err %v", e.Action, err)
	}

	if err := h.r.doBulk(reqs); err != nil {
		log.Errorf("do ES bulks err %v, stop", err)
		return canal.ErrHandleInterrupted
	}

	return nil
}

func (h *rowsEventHandler) String() string {
	return "ESRiverRowsEventHandler"
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id, Parent: parentID}

		if action == canal.DeleteAction {
			req.Action = elastic.ActionDelete
			r.st.DeleteNum.Add(1)
		} else {
			r.makeInsertReqData(req, rule, values)
			r.st.InsertNum.Add(1)
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = r.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = r.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: beforeID, Parent: beforeParentID}

		if beforeID != afterID || beforeParentID != afterParentID {
			req.Action = elastic.ActionDelete
			reqs = append(reqs, req)

			req = &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: afterID, Parent: afterParentID}
			r.makeInsertReqData(req, rule, rows[i+1])

			r.st.DeleteNum.Add(1)
			r.st.InsertNum.Add(1)
		} else {
			r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
			r.st.UpdateNum.Add(1)
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	}

	return value
}

func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}

func (r *River) makeInsertReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = elastic.ActionIndex

	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				v := r.makeReqColumnData(&c, values[i])
				if fieldType == fieldTypeList {
					if str, ok := v.(string); ok {
						req.Data[elastic] = strings.Split(str, ",")
					} else {
						req.Data[elastic] = v
					}
				} else {
					req.Data[elastic] = v
				}
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
		}
	}
}

func (r *River) makeUpdateReqData(req *elastic.BulkRequest, rule *Rule,
	beforeValues []interface{}, afterValues []interface{}) {
	req.Data = make(map[string]interface{}, len(beforeValues))

	// maybe dangerous if something wrong delete before?
	req.Action = elastic.ActionUpdate

	for i, c := range rule.TableInfo.Columns {
		mapped := false
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				// has custom field mapping
				v := r.makeReqColumnData(&c, afterValues[i])
				str, ok := v.(string)
				if ok == false {
					req.Data[c.Name] = v
				} else {
					if fieldType == fieldTypeList {
						req.Data[elastic] = strings.Split(str, ",")
					} else {
						req.Data[elastic] = str
					}
				}
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		}

	}
}

// Get primary keys in one row and format them into a string
// PK must not be nil
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	pks, err := canal.GetPKValues(rule.TableInfo, row)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range pks {
		if value == nil {
			return "", errors.Errorf("The %ds PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

func (r *River) doBulk(reqs []*elastic.BulkRequest) error {
	flag := true
	var err error
	if len(reqs) == 0 {
		return nil
	}
	if len(reqs) == 1{
		switch reqs[0].Action {
		case "index":
			err = r.es.Update(reqs[0].Index, reqs[0].Type, reqs[0].ID, reqs[0].Data)
		case "delete":
			err = r.es.Delete(reqs[0].Index, reqs[0].Type, reqs[0].ID)
		case "update":
			err = r.es.Update(reqs[0].Index, reqs[0].Type, reqs[0].ID, reqs[0].Data)
		default:
			flag = false
			err = nil
		}
		if flag {
			if err != nil {
				log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
				return errors.Trace(err)
			}
			return nil
		}
	} else if len(reqs) > 1 || (!flag){
		if resp, err := r.es.Bulk(reqs); err != nil {
			log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
			return errors.Trace(err)
		} else if resp.Errors {
			for i := 0; i < len(resp.Items); i++ {
				for action, item := range resp.Items[i] {
					if len(item.Error) > 0 {
						log.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
							action, item.Index, item.Type, item.ID, item.Status, item.Error)
					}
				}
			}
		}
	}

	return nil
}
