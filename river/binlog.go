package river

import (
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
	"time"
)

func (r *River) syncBinlog() error {
	var pos mysql.Position

	s, err := r.syncer.StartSync(mysql.Position{r.m.Name, r.m.Position})
	if err != nil {
		return fmt.Errorf("start sync replication at %s:%d error %v", r.m.Name, r.m.Position, err)
	}

	lastTime := time.Now()
	for {
		//current binlog pos
		pos.Name = r.m.Name
		pos.Pos = r.m.Position

		ev, err := s.GetEvent()
		if err != nil {
			return fmt.Errorf("get event error %v", err)
		}

		//next binlog pos
		r.m.Position = ev.Header.LogPos

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			r.m.Update(string(e.NextLogName), uint32(e.Position))
			log.Infof("rotate binlog to (%s, %d)", r.m.Name, r.m.Position)
			r.m.Save()
		case *replication.RowsEvent:
			// we only focus row based event
			if err = r.handleRowsEvent(pos, ev); err != nil {
				log.Errorf("handle rows event error %v", err)
			}

			// Warn, sync data is asynchronous, if crashed, saved binlog position may be not correct.
			// change later or only log???
			n := time.Now()
			if n.Sub(lastTime) > 1*time.Second {
				lastTime = n
				r.m.Save()
			}
		default:
			//skip
		}

	}

	return nil
}

func (r *River) handleRowsEvent(pos mysql.Position, e *replication.BinlogEvent) error {
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
		return r.syncDocument(pos, rule, syncInsertDoc, ev.Rows)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return r.syncDocument(pos, rule, syncDeleteDoc, ev.Rows)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return r.syncDocument(pos, rule, syncUpdateDoc, ev.Rows)
	default:
		return fmt.Errorf("%s not supported now", e.Header.EventType)
	}
}
