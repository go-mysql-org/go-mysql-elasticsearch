package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql-elasticsearch/river"
)

var configFile = flag.String("config", "./etc/river.toml", "go-mysql-elasticsearch config file")
var my_addr = flag.String("my_addr", "", "MySQL addr")
var my_user = flag.String("my_user", "", "MySQL user")
var my_pass = flag.String("my_pass", "", "MySQL password")
var my_charset = flag.String("my_charset", "", "MySQL character set")
var es_addr = flag.String("es_addr", "", "Elasticsearch addr")
var data_dir = flag.String("data_dir", "", "path for go-mysql-elasticsearch to save data")
var server_id = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
var flavor = flag.String("flavor", "", "flavor: mysql or mariadb")
var execution = flag.String("exec", "", "mysqldump execution path")
var logLevel = flag.String("log_level", "info", "log level")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	log.SetLevelByString(*logLevel)

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
		println(errors.ErrorStack(err))
		return
	}

	if len(*my_addr) > 0 {
		cfg.MyAddr = *my_addr
	}

	if len(*my_user) > 0 {
		cfg.MyUser = *my_user
	}

	if len(*my_pass) > 0 {
		cfg.MyPassword = *my_pass
	}

	if len(*my_charset) > 0 {
		cfg.MyCharset = *my_charset
	}

	if *server_id > 0 {
		cfg.ServerID = uint32(*server_id)
	}

	if len(*es_addr) > 0 {
		cfg.ESAddr = *es_addr
	}

	if len(*data_dir) > 0 {
		cfg.DataDir = *data_dir
	}

	if len(*flavor) > 0 {
		cfg.Flavor = *flavor
	}

	if len(*execution) > 0 {
		cfg.DumpExec = *execution
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	r.Start()

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	r.Close()
}
