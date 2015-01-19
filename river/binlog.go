package river

import (
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
)

func (r *River) syncBinlog() error {
	s, err := r.syncer.StartSync(mysql.Position{r.m.Name, uint32(r.m.Position)})
	if err != nil {
		return fmt.Errorf("start sync replication at %s:%d error %v", r.m.Name, r.m.Position, err)
	}

	for {
		event, err := s.GetEvent()
		if err != nil {
			return fmt.Errorf("get event error %v", err)
		}

		r.m.Position = uint64(event.Header.LogPos)

		switch e := event.Event.(type) {
		case *replication.RotateEvent:
			r.m.Name = string(e.NextLogName)
			r.m.Position = e.Position
		case *replication.RowsEvent:
			// we only focus row based event
			if err = r.handleRowsEvent(event); err != nil {
				log.Errorf("handle rows event error %v", err)
			}
		default:
			//skip
		}

		// todo, use mmap to fast it
		if err = r.m.Save(r.masterInfoPath()); err != nil {
			log.Errorf("save master info error %v", err)
		}
	}

	return nil
}

func (r *River) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1:
		return r.handleWriteRowsEvent(ev)
	case replication.DELETE_ROWS_EVENTv1:
		return r.handleDeleteRowsEvent(ev)
	case replication.UPDATE_ROWS_EVENTv1:
		return r.handleUpdateRowsEvent(ev)
	case replication.WRITE_ROWS_EVENTv2:
		return r.handleWriteRowsEvent(ev)
	case replication.UPDATE_ROWS_EVENTv2:
		return r.handleUpdateRowsEvent(ev)
	case replication.DELETE_ROWS_EVENTv2:
		return r.handleDeleteRowsEvent(ev)
	default:
		return fmt.Errorf("%s not supported now", e.Header.EventType)
	}
}

func (r *River) handleWriteRowsEvent(e *replication.RowsEvent) error {
	rule := r.getRuleWithRowsEvent(e)

	for _, values := range e.Rows {
		if err := r.syncDocument(rule, syncInsertDoc, values, nil); err != nil {
			return err
		}
	}

	return nil
}

func (r *River) handleDeleteRowsEvent(e *replication.RowsEvent) error {
	rule := r.getRuleWithRowsEvent(e)

	for _, values := range e.Rows {
		if err := r.syncDocument(rule, syncDeleteDoc, values, nil); err != nil {
			return err
		}
	}

	return nil
}

func (r *River) handleUpdateRowsEvent(e *replication.RowsEvent) error {
	if len(e.Rows)%2 != 0 {
		return fmt.Errorf("invalid update rows event, must have 2x rows, but %d", len(e.Rows))
	}

	rule := r.getRuleWithRowsEvent(e)

	for i := 0; i < len(e.Rows); i += 2 {
		if err := r.syncDocument(rule, syncUpdateDoc, e.Rows[i], e.Rows[i+1]); err != nil {
			return err
		}
	}

	return nil
}

func (r *River) getRuleWithRowsEvent(e *replication.RowsEvent) *Rule {
	// Caveat: table may be altered at runtime.
	schema := string(e.Table.Schema)
	table := string(e.Table.Table)
	return r.rules[ruleKey(schema, table)]
}
