// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql-elasticsearch/pkg/gtid"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	tmysql "github.com/pingcap/parser/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

var (
	// for MariaDB, UUID set as `gtid_domain_id` + domainServerIDSeparator + `server_id`
	domainServerIDSeparator = "-"
)

// GetMasterStatus gets status from master
func GetMasterStatus(db *sql.DB, flavor string) (gmysql.Position, gtid.Set, error) {
	var (
		binlogPos gmysql.Position
		gs        gtid.Set
	)

	rows, err := db.Query(`SHOW MASTER STATUS`)
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}

	// Show an example.
	/*
		MySQL [test]> SHOW MASTER STATUS;
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                          |
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| ON.000001 |     4822 |              |                  | 85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46
		+-----------+----------+--------------+------------------+--------------------------------------------+
	*/
	var (
		gtidStr    string
		binlogName string
		pos        uint32
		nullPtr    interface{}
	)
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr, &gtidStr)
		} else {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr)
		}
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}

		binlogPos = gmysql.Position{
			Name: binlogName,
			Pos:  pos,
		}

		gs, err = gtid.ParserGTID(flavor, gtidStr)
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}
	}
	if rows.Err() != nil {
		return binlogPos, gs, errors.Trace(rows.Err())
	}

	if flavor == gmysql.MariaDBFlavor && (gs == nil || gs.String() == "") {
		gs, err = GetMariaDBGTID(db)
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}
	}

	return binlogPos, gs, nil
}

// GetMariaDBGTID gets MariaDB's `gtid_binlog_pos`
// it can not get by `SHOW MASTER STATUS`
func GetMariaDBGTID(db *sql.DB) (gtid.Set, error) {
	gtidStr, err := GetGlobalVariable(db, "gtid_binlog_pos")
	if err != nil {
		return nil, errors.Trace(err)
	}
	gs, err := gtid.ParserGTID(gmysql.MariaDBFlavor, gtidStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gs, nil
}

// GetGlobalVariable gets server's global variable
func GetGlobalVariable(db *sql.DB, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", variable)
	rows, err := db.Query(query)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	for rows.Next() {
		err = rows.Scan(&variable, &value)
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return "", errors.Trace(rows.Err())
	}

	return value, nil
}

// GetServerID gets server's `server_id`
func GetServerID(db *sql.DB) (int64, error) {
	serverIDStr, err := GetGlobalVariable(db, "server_id")
	if err != nil {
		return 0, errors.Trace(err)
	}

	serverID, err := strconv.ParseInt(serverIDStr, 10, 64)
	return serverID, errors.Trace(err)
}

// GetMariaDBGtidDomainID gets MariaDB server's `gtid_domain_id`
func GetMariaDBGtidDomainID(db *sql.DB) (uint32, error) {
	domainIDStr, err := GetGlobalVariable(db, "gtid_domain_id")
	if err != nil {
		return 0, errors.Trace(err)
	}

	domainID, err := strconv.ParseUint(domainIDStr, 10, 32)
	return uint32(domainID), errors.Trace(err)
}

// GetServerUUID gets server's `server_uuid`
func GetServerUUID(db *sql.DB, flavor string) (string, error) {
	if flavor == gmysql.MariaDBFlavor {
		return GetMariaDBUUID(db)
	}
	serverUUID, err := GetGlobalVariable(db, "server_uuid")
	return serverUUID, errors.Trace(err)
}

// GetMariaDBUUID gets equivalent `server_uuid` for MariaDB
// `gtid_domain_id` joined `server_id` with domainServerIDSeparator
func GetMariaDBUUID(db *sql.DB) (string, error) {
	domainID, err := GetMariaDBGtidDomainID(db)
	if err != nil {
		return "", errors.Trace(err)
	}
	serverID, err := GetServerID(db)
	if err != nil {
		return "", errors.Trace(err)
	}
	return fmt.Sprintf("%d%s%d", domainID, domainServerIDSeparator, serverID), nil
}

// GetSQLMode returns sql_mode.
func GetSQLMode(db *sql.DB) (tmysql.SQLMode, error) {
	sqlMode, err := GetGlobalVariable(db, "sql_mode")
	if err != nil {
		return tmysql.ModeNone, errors.Trace(err)
	}

	mode, err := tmysql.GetSQLMode(sqlMode)
	return mode, errors.Trace(err)
}

// HasAnsiQuotesMode checks whether database has `ANSI_QUOTES` set
func HasAnsiQuotesMode(db *sql.DB) (bool, error) {
	mode, err := GetSQLMode(db)
	if err != nil {
		return false, errors.Trace(err)
	}
	return mode.HasANSIQuotesMode(), nil
}

// GetParser gets a parser which maybe enabled `ANSI_QUOTES` sql_mode
func GetParser(db *sql.DB, ansiQuotesMode bool) (*parser.Parser, error) {
	if !ansiQuotesMode {
		// try get from DB
		var err error
		ansiQuotesMode, err = HasAnsiQuotesMode(db)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	parser2 := parser.New()
	if ansiQuotesMode {
		parser2.SetSQLMode(tmysql.ModeANSIQuotes)
	}
	return parser2, nil
}

// KillConn kills the DB connection (thread in mysqld)
func KillConn(db *sql.DB, connID uint32) error {
	_, err := db.Exec(fmt.Sprintf("KILL %d", connID))
	return errors.Trace(err)
}

// IsMySQLError checks whether err is MySQLError error
func IsMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// IsErrBinlogPurged checks whether err is BinlogPurged error
func IsErrBinlogPurged(err error) bool {
	err = errors.Cause(err)
	e, ok := err.(*gmysql.MyError)
	return ok && e.Code == tmysql.ErrMasterFatalErrorReadingBinlog
}

// IsErrTableNotExists checks whether err is TableNotExists error
func IsErrTableNotExists(err error) bool {
	return IsMySQLError(err, tmysql.ErrNoSuchTable)
}

// IsErrDupEntry checks whether err is DupEntry error
func IsErrDupEntry(err error) bool {
	return IsMySQLError(err, tmysql.ErrDupEntry)
}

// IsNoSuchThreadError checks whether err is NoSuchThreadError
func IsNoSuchThreadError(err error) bool {
	return IsMySQLError(err, tmysql.ErrNoSuchThread)
}
