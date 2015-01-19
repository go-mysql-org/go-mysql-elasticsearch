package main

import (
	"flag"
	"github.com/siddontang/go-mysql-elasticsearch/river"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var configFile = flag.String("config", "./etc/river.toml", "go-mysql-elasticsearch config file")
var my_addr = flag.String("my_addr", "", "MySQL addr")
var es_addr = flag.String("es_addr", "", "Elasticsearch addr")
var data_dir = flag.String("data_dir", "", "path for go-mysql-elasticsearch to save data")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(err.Error())
		return
	}

	if len(*my_addr) > 0 {
		cfg.MyAddr = *my_addr
	}

	if len(*es_addr) > 0 {
		cfg.ESAddr = *es_addr
	}

	if len(*data_dir) > 0 {
		cfg.DataDir = *data_dir
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(err.Error())
		return
	}

	go func() {
		<-sc
		r.Close()
	}()

	r.Run()
}
