package river

import (
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
	"time"
)

func (r *River) syncBinlog() error {
	s, err := r.syncer.StartSync(mysql.Position{r.m.Name, uint32(r.m.Position)})
	if err != nil {
		return fmt.Errorf("start sync replication at %s:%d error %v", r.m.Name, r.m.Position, err)
	}

	lastTime := time.Now()
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
			if err = r.m.Save(r.masterInfoPath()); err != nil {
				log.Errorf("save master info error %v", err)
			}
		case *replication.RowsEvent:
			// we only focus row based event
			if err = r.handleRowsEvent(event); err != nil {
				log.Errorf("handle rows event error %v", err)
			}
		default:
			//skip
		}

		n := time.Now()
		if n.Sub(lastTime) > time.Second {
			lastTime = n
			if err = r.m.Save(r.masterInfoPath()); err != nil {
				log.Errorf("save master info error %v", err)
			}
		}
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
		log.Infof("no rule for %s.%s rows event, skip", schema, table)
	}

	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return r.syncDocument(rule, syncInsertDoc, ev.Rows)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return r.syncDocument(rule, syncDeleteDoc, ev.Rows)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return r.syncDocument(rule, syncUpdateDoc, ev.Rows)
	default:
		return fmt.Errorf("%s not supported now", e.Header.EventType)
	}
}
