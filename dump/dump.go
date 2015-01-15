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

	// Key is db name, value is tables, will override Databases
	Tables map[string][]string

	Databases []string
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
	d.Tables = make(map[string][]string, 16)
	d.Databases = make([]string, 0, 16)

	return d, nil
}

func (d *Dumper) AddDatabase(db string) {
	d.Databases = append(d.Databases, db)
}

func (d *Dumper) AddTable(db string, table string) {
	ts, ok := d.Tables[db]
	if !ok {
		ts = []string{table}
	} else {
		ts = append(ts, table)
	}
	d.Tables[db] = []string{table}
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
	args = append(args, "--compact")
	args = append(args, "--skip-add-locks")

	// Multi row is easy for us to parse the data
	args = append(args, "--skip-extended-insert")

	if len(d.Tables) == 0 && len(d.Databases) == 0 {
		args = append(args, "--all-databases")
	} else if len(d.Tables) == 0 {
		args = append(args, "--databases")
		args = append(args, d.Databases...)
	} else {
		// tables will override databases
		if len(d.Tables) == 1 {
			for db, tables := range d.Tables {
				args = append(args, db)
				args = append(args, tables...)
			}
		} else {
			args = append(args, "--tables")
			for db, tables := range d.Tables {
				for _, table := range tables {
					args = append(args, fmt.Sprintf("%s.%s", db, table))
				}
			}
		}
	}

	cmd := exec.Command(d.ExecutionPath, args...)

	cmd.Stderr = os.Stderr
	cmd.Stdout = w

	return cmd.Run()
}
