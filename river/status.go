package river

import (
	"bytes"
	"fmt"
	"net"
	"net/http"

	"github.com/siddontang/go/log"
	"github.com/siddontang/go/sync2"
)

type stat struct {
	r *River

	l net.Listener

	InsertNum sync2.AtomicInt64
	UpdateNum sync2.AtomicInt64
	DeleteNum sync2.AtomicInt64
}

func (s *stat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer

	rr, err := s.r.canal.Execute("SHOW MASTER STATUS")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("execute sql error %v", err)))
		return
	}

	binName, _ := rr.GetString(0, 0)
	binPos, _ := rr.GetUint(0, 1)

	pos := s.r.canal.SyncedPosition()

	buf.WriteString(fmt.Sprintf("server_current_binlog:(%s, %d)\n", binName, binPos))
	buf.WriteString(fmt.Sprintf("read_binlog:%s\n", pos))

	buf.WriteString(fmt.Sprintf("insert_num:%d\n", s.InsertNum.Get()))
	buf.WriteString(fmt.Sprintf("update_num:%d\n", s.UpdateNum.Get()))
	buf.WriteString(fmt.Sprintf("delete_num:%d\n", s.DeleteNum.Get()))

	w.Write(buf.Bytes())
}

func (s *stat) Run(addr string) {
	if len(addr) == 0 {
		return
	}
	log.Infof("run status http server %s", addr)
	var err error
	s.l, err = net.Listen("tcp", addr)
	if err != nil {
		log.Errorf("listen stat addr %s err %v", addr, err)
		return
	}

	srv := http.Server{}
	mux := http.NewServeMux()
	mux.Handle("/stat", s)
	srv.Handler = mux

	srv.Serve(s.l)
}

func (s *stat) Close() {
	if s.l != nil {
		s.l.Close()
	}
}
