package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	log "github.com/sirupsen/logrus"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		string(e.NextLogName),
		uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
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
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	reqs := make([]*elastic.BulkRequest, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []*elastic.BulkRequest:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do ES bulk err %v, close sync", err)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
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

		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id, Parent: parentID, Pipeline: rule.Pipeline}

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

			req = &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: afterID, Parent: afterParentID, Pipeline: rule.Pipeline}
			r.makeInsertReqData(req, rule, rows[i+1])

			r.st.DeleteNum.Add(1)
			r.st.InsertNum.Add(1)
		} else {
			if len(rule.Pipeline) > 0 {
				// Pipelines can only be specified on index action
				r.makeInsertReqData(req, rule, rows[i+1])
				// Make sure action is index, not create
				req.Action = elastic.ActionIndex
				req.Pipeline = rule.Pipeline
			} else {
				r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
			}
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
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, _ := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			return vt.Format(time.RFC3339)
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
	//fmt.Printf("%#v\n", rule.TableInfo)
	//定义全局字符串存储
	var spercontact string
	var si_zwnk string
	var si_id string
	var percontact_r schema.TableColumn
	var si_zwnk_r schema.TableColumn
	var si_id_r schema.TableColumn
	var i_partprice schema.TableColumn
	var i_price schema.TableColumn
	var i_boxprice schema.TableColumn
	var v_discount_acmount schema.TableColumn
	var v_platform_rate schema.TableColumn
	var v_shop_rate schema.TableColumn
	var v_fee schema.TableColumn
	var v_shopfee schema.TableColumn
	var v_atotalprice schema.TableColumn
	var v_asid schema.TableColumn
	var v_aorderstatus schema.TableColumn
	var v_aend_deliverytime schema.TableColumn
	var sdatabase string

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
				req.Data[elastic] = r.getFieldValue(&c, fieldType, values[i])
			}
		}

		if rule.TableInfo.Name == "alp_merchant_order" {
			if c.Name == "totalprice" {
				i_partprice = c
				i_price = c
				i_boxprice = c
				v_discount_acmount = c
				v_platform_rate = c
				v_shop_rate = c
				v_fee = c
			}
			//添加拼接字符串处理
			switch c.Name {
			case "contact":
				si_zwnk = fmt.Sprintf("%s%s-", si_zwnk, values[i])
			case "mobile":
				si_zwnk = fmt.Sprintf("%s%s-", si_zwnk, values[i])
			case "address":
				si_zwnk = fmt.Sprintf("%s%s", si_zwnk, values[i])
				si_zwnk_r = c
			}
		}

		if rule.TableInfo.Name == "alp_dish_sales" {
			//添加拼接字符串处理
			switch c.Name {
			case "category_name":
				spercontact = fmt.Sprintf("%s%s_", spercontact, values[i])
			case "dish_name":
				spercontact = fmt.Sprintf("%s%s_", spercontact, values[i])
				percontact_r = c
			case "sku_name":
				spercontact = fmt.Sprintf("%s%s_", spercontact, values[i])
			case "dishsno":
				spercontact = fmt.Sprintf("%s%s", spercontact, values[i])
			}
		}

		if rule.TableInfo.Name == "alp_merchant_order_activity" {
			//添加拼接字符串处理
			switch c.Name {
			case "activity_type":
				si_id = fmt.Sprintf("%s%s_", si_id, values[i])
			case "discount_acmount":
				si_id = fmt.Sprintf("%s%s_", si_id, values[i])
			case "shop_rate":
				si_id = fmt.Sprintf("%s%s_", si_id, values[i])
				v_shopfee = c
				v_atotalprice = c
			case "source":
				si_id = fmt.Sprintf("%s%s", si_id, values[i])
			}
			if c.Name == "activity_name" {
				si_id_r = c
			}
			if c.Name == "amwaid" {
				v_asid = c
				v_aorderstatus = c
			}
			if c.Name == "utime" {
				v_aend_deliverytime = c
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
		}
	}

	//定制化需求
	if rule.TableInfo.Name == "alp_merchant_order" {
		for _, s := range r.c.Sources {
			sdatabase = s.Schema
		}
		time.Sleep(1000 * time.Millisecond)
		conn, _ := client.Connect(r.c.MyAddr, r.c.MyUser, r.c.MyPassword, sdatabase)
		conn.Ping()
		// ress, err := conn.Execute("select amoid from alp_merchant_order order by amoid desc limit 0,1")
		// amoid, _ := ress.GetIntByName(0, "amoid")
		s_amoid := "select sum(if(partstatus=2,partprice,0)) partprice,sum(price) price ,sum(if(itemname='餐盒费',price,0)) boxprice from alp_merchant_order_item where amoid=" + req.ID
		//fmt.Printf("%s", s_amoid)
		res, err := conn.Execute(s_amoid)
		if err != nil {
			log.Errorf("err %v ", err)
		}
		partprice, _ := res.GetFloatByName(0, "partprice")
		price, _ := res.GetFloatByName(0, "price")
		boxprice, _ := res.GetFloatByName(0, "boxprice")
		f_partprice := fmt.Sprintf("%0.2f", partprice)
		f_price := fmt.Sprintf("%0.2f", price)
		f_boxprice := fmt.Sprintf("%0.2f", boxprice)
		i_partprice.Name = "i_partprice"
		i_price.Name = "price"
		i_boxprice.Name = "i_boxprice"
		a_amoid := "select sum(discount_acmount) discount_acmount,sum(platform_rate) platform_rate,sum(shop_rate) shop_rate from alp_merchant_order_activity  where amoid=" + req.ID
		rss, err := conn.Execute(a_amoid)
		discount_acmount, _ := rss.GetFloatByName(0, "discount_acmount")
		platform_rate, _ := rss.GetFloatByName(0, "platform_rate")
		shop_rate, _ := rss.GetFloatByName(0, "shop_rate")
		f_discount_acmount := fmt.Sprintf("%0.2f", discount_acmount)
		f_platform_rate := fmt.Sprintf("%0.2f", platform_rate)
		f_shop_rate := fmt.Sprintf("%0.2f", shop_rate)
		v_discount_acmount.Name = "v_discount_acmount"
		v_platform_rate.Name = "v_platform_rate"
		v_shop_rate.Name = "v_shop_rate"
		//e_amoid := "select sum(fee) fee from alp_merchant_order_extras where amoid=" + strconv.FormatInt(amoid, 10)
		e_amoid := "select sum(fee) fee from alp_merchant_order_extras where amoid=" + req.ID
		rse, err := conn.Execute(e_amoid)
		fee, _ := rse.GetFloatByName(0, "fee")
		f_fee := fmt.Sprintf("%0.2f", fee)
		v_fee.Name = "v_fee"
		req.Data["i_partprice"] = r.makeReqColumnData(&i_partprice, f_partprice)
		req.Data["price"] = r.makeReqColumnData(&i_price, f_price)
		req.Data["i_boxprice"] = r.makeReqColumnData(&i_boxprice, f_boxprice)
		req.Data["v_discount_acmount"] = r.makeReqColumnData(&v_discount_acmount, f_discount_acmount)
		req.Data["v_platform_rate"] = r.makeReqColumnData(&v_platform_rate, f_platform_rate)
		req.Data["v_shop_rate"] = r.makeReqColumnData(&v_shop_rate, f_shop_rate)
		req.Data["v_fee"] = r.makeReqColumnData(&v_fee, f_fee)
		conn.Close()
	}

	//定制化需求
	if rule.TableInfo.Name == "alp_merchant_order_activity" {
		for _, s := range r.c.Sources {
			sdatabase = s.Schema
		}
		time.Sleep(1000 * time.Millisecond)
		aconn, _ := client.Connect(r.c.MyAddr, r.c.MyUser, r.c.MyPassword, sdatabase)
		aconn.Ping()
		aress, err := aconn.Execute("select amoid from alp_merchant_order_activity where amoaid =" + req.ID)
		aamoid, _ := aress.GetIntByName(0, "amoid")
		as_amoid := "select ifnull(shopfee,0) as shopfee,totalprice+ifnull(order_delivery_pay,0)-ifnull(delivery_pay,0) as totalprice,sid,orderstatus,end_deliverytime from alp_merchant_order where amoid=" + strconv.FormatInt(aamoid, 10)
		//fmt.Printf("%s", s_amoid)
		ares, err := aconn.Execute(as_amoid)
		if err != nil {
			log.Errorf("err %v ", err)
		}
		shopfee, _ := ares.GetFloatByName(0, "shopfee")
		atotalprice, _ := ares.GetFloatByName(0, "totalprice")
		asid, _ := ares.GetIntByName(0, "sid")
		aorderstatus, _ := ares.GetIntByName(0, "orderstatus")
		aend_deliverytime, _ := ares.GetStringByName(0, "end_deliverytime")
		f_shopfee := fmt.Sprintf("%0.2f", shopfee)
		f_atotalprice := fmt.Sprintf("%0.2f", atotalprice)
		t, _ := time.Parse("2006-01-02 15:04:05", aend_deliverytime)
		t_aend_deliverytime := t.Unix()
		v_shopfee.Name = "shopfee"
		v_atotalprice.Name = "totalprice"
		v_asid.Name = "sid"
		v_aorderstatus.Name = "orderstatus"
		v_aend_deliverytime.Name = "end_deliverytime"
		req.Data["shopfee"] = r.makeReqColumnData(&v_shopfee, f_shopfee)
		req.Data["totalprice"] = r.makeReqColumnData(&v_atotalprice, f_atotalprice)
		req.Data["sid"] = r.makeReqColumnData(&v_asid, asid)
		req.Data["orderstatus"] = r.makeReqColumnData(&v_aorderstatus, aorderstatus)
		req.Data["end_deliverytime"] = r.makeReqColumnData(&v_aend_deliverytime, t_aend_deliverytime)
		//添加插入处理
		si_id_r.Name = "id"
		req.Data["id"] = r.makeReqColumnData(&si_id_r, si_id)
		aconn.Close()
	}

	if rule.TableInfo.Name == "alp_dish_sales" {
		//添加插入处理
		percontact_r.Name = "percontact"
		req.Data["percontact"] = r.makeReqColumnData(&percontact_r, spercontact)
	}
	if rule.TableInfo.Name == "alp_merchant_order" {
		//添加插入处理
		si_zwnk_r.Name = "i_zwnk"
		req.Data["i_zwnk"] = r.makeReqColumnData(&si_zwnk_r, si_zwnk)
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
				req.Data[elastic] = r.getFieldValue(&c, fieldType, afterValues[i])

			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		}

	}
}

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
		ids []interface{}
		err error
	)
	if rule.ID == nil {
		ids, err = canal.GetPKValues(rule.TableInfo, row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := canal.GetColumnValue(rule.TableInfo, column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
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
	if len(reqs) == 0 {
		return nil
	}

	if resp, err := r.es.Bulk(reqs); err != nil {
		log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	} else if resp.Code/100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}

// get mysql field value and convert it to specific value to es
func (r *River) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case fieldTypeList:
		v := r.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}

	case fieldTypeDate:
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = r.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	}

	if fieldValue == nil {
		fieldValue = r.makeReqColumnData(col, value)
	}
	return fieldValue
}
