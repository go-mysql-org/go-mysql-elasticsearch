package dump

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

type Dumper struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	ExecutionPath string

	Addr     string
	User     string
	Password string

	// Will override Databases
	Tables  []string
	TableDB string

	Databases []string

	IgnoreTables map[string][]string
}

func NewDumper(executionPath string, addr string, user string, password string) (*Dumper, error) {
	path, err := exec.LookPath(executionPath)
	if err != nil {
		return nil, err
	}

	d := new(Dumper)
	d.ExecutionPath = path
	d.Addr = addr
	d.User = user
	d.Password = password
	d.Tables = make([]string, 0, 16)
	d.Databases = make([]string, 0, 16)
	d.IgnoreTables = make(map[string][]string)

	return d, nil
}

func (d *Dumper) AddDatabase(dbs ...string) {
	d.Databases = append(d.Databases, dbs...)
}

func (d *Dumper) AddTable(db string, tables ...string) {
	if d.TableDB != db {
		d.TableDB = db
		d.Tables = d.Tables[0:0]
	}

	d.Tables = append(d.Tables, tables...)
}

func (d *Dumper) AddIgnoreTables(db string, tables ...string) {
	t, _ := d.IgnoreTables[db]
	t = append(t, tables...)
	d.IgnoreTables[db] = t
}

func (d *Dumper) Dump(w io.Writer) error {
	args := make([]string, 0, 16)

	// Common args
	seps := strings.Split(d.Addr, ":")
	args = append(args, fmt.Sprintf("--host=%s", seps[0]))
	if len(seps) > 1 {
		args = append(args, fmt.Sprintf("--port=%s", seps[1]))
	}

	args = append(args, fmt.Sprintf("--user=%s", d.User))
	args = append(args, fmt.Sprintf("--password=%s", d.Password))

	args = append(args, "--master-data=2")
	args = append(args, "--single-transaction")
	args = append(args, "--skip-lock-tables")

	// Disable uncessary data
	args = append(args, "--compact")
	args = append(args, "--skip-opt")

	// We only care about data
	args = append(args, "--no-create-info")

	// Multi row is easy for us to parse the data
	args = append(args, "--skip-extended-insert")

	for db, tables := range d.IgnoreTables {
		for _, table := range tables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", db, table))
		}
	}

	if len(d.Tables) == 0 && len(d.Databases) == 0 {
		args = append(args, "--all-databases")
	} else if len(d.Tables) == 0 {
		args = append(args, "--databases")
		args = append(args, d.Databases...)
	} else {
		args = append(args, d.TableDB)
		args = append(args, d.Tables...)

		// If we only dump some tables, the dump data will not have database name
		// which makes us hard to parse, so here we add it manually.

		w.Write([]byte(fmt.Sprintf("USE `%s`;\n", d.TableDB)))
	}

	cmd := exec.Command(d.ExecutionPath, args...)

	cmd.Stderr = os.Stderr
	cmd.Stdout = w

	return cmd.Run()
}
