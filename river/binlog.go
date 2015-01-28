package river

import (
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
)

func (r *River) syncBinlog() error {
	pos := mysql.Position{r.m.Name, r.m.Position}

	log.Infof("start sync binlog at %v", pos)

	s, err := r.syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("start sync replication at %v error %v", pos, err)
	}

	for {
		ev, err := s.GetEvent()
		if err != nil {
			return err
		}

		//next binlog pos
		pos.Pos = ev.Header.LogPos

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			r.ev <- pos
			log.Infof("rotate binlog to %v", pos)
		case *replication.RowsEvent:
			// we only focus row based event
			if err = r.handleRowsEvent(ev); err != nil {
				log.Errorf("handle rows event error %v", err)
			}
		default:
			//skip
		}

		r.ev <- pos
	}

	return nil
}

func (r *River) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	rule, ok := r.rules[ruleKey(schema, table)]
	if !ok {
		return nil
	}

	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return r.syncDocument(rule, syncInsertDoc, ev.Rows, true)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return r.syncDocument(rule, syncDeleteDoc, ev.Rows, true)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return r.syncDocument(rule, syncUpdateDoc, ev.Rows, true)
	default:
		return fmt.Errorf("%s not supported now", e.Header.EventType)
	}
}
