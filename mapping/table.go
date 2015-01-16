package mapping

import (
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/schema"
)

func (r *Rule) FetchTableInfo(conn *client.Conn) error {
	var err error
	r.TableInfo, err = schema.NewTable(conn, r.Schema, r.Table)
	if err != nil {
		return err
	}

	for _, column := range r.TableInfo.Columns {
		// if no custom field mapping, use column name
		if _, ok := r.FieldMapping[column.Name]; !ok {
			r.FieldMapping[column.Name] = column.Name
		}

	}

	return nil
}
