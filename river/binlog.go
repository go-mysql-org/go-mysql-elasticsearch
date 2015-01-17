package river

import (
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
)

func (r *River) sync() error {
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

func (r *River) handleRowEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1:
		return r.handleWriteRowEvent(ev)
	case replication.DELETE_ROWS_EVENTv1:
		return r.handleDeleteRowEvent(ev)
	case replication.UPDATE_ROWS_EVENTv1:
		return r.handleUpdateRowEvent(ev)
	case replication.WRITE_ROWS_EVENTv2:
		return r.handleWriteRowEvent(ev)
	case replication.UPDATE_ROWS_EVENTv2:
		return r.handleUpdateRowEvent(ev)
	case replication.DELETE_ROWS_EVENTv2:
		return r.handleDeleteRowEvent(ev)
	default:
		return fmt.Errorf("%s not supported now", e.Header.EventType)
	}
}

func (r *River) handleWriteRowEvent(e *replication.RowsEvent) error {
	return nil
}

func (r *River) handleDeleteRowEvent(e *replication.RowsEvent) error {
	return nil
}

func (r *River) handleUpdateRowEvent(e *replication.RowsEvent) error {
	return nil
}
