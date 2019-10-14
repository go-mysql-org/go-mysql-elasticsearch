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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"
)

// not support to config yet
var (
	UUIDIndexFilename  = "server-uuid.index"
	MetaFilename       = "relay.meta"
	uuidIndexSeparator = "."
)

// ParseUUIDIndex parses server-uuid.index
func ParseUUIDIndex(indexPath string) ([]string, error) {
	fd, err := os.Open(indexPath)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	uuids := make([]string, 0, 5)
	br := bufio.NewReader(fd)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			if len(line) == 0 {
				break
			}
		} else if err != nil {
			return nil, errors.Trace(err)
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		uuids = append(uuids, line)
	}

	return uuids, nil
}

// AddSuffixForUUID adds a suffix for UUID
func AddSuffixForUUID(uuid string, ID int) string {
	return fmt.Sprintf("%s%s%06d", uuid, uuidIndexSeparator, ID) // eg. 53ea0ed1-9bf8-11e6-8bea-64006a897c73.000001
}

// SuffixIntToStr convert int-represented suffix to string-represented
func SuffixIntToStr(ID int) string {
	return fmt.Sprintf("%06d", ID)
}

// ParseSuffixForUUID parses UUID (with suffix) to (UUID without suffix, suffix) pair
func ParseSuffixForUUID(uuid string) (string, int, error) {
	parts := strings.Split(uuid, uuidIndexSeparator)
	if len(parts) != 2 || len(parts[1]) != 6 {
		return "", 0, errors.NotValidf("UUID (with suffix) %s", uuid)
	}
	ID, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, errors.NotValidf("UUID (with suffix) %s", uuid)
	}
	return parts[0], ID, nil
}

// GetSuffixUUID gets UUID (with suffix) by UUID (without suffix)
// when multi UUIDs (without suffix) are the same, the newest will be return
func GetSuffixUUID(indexPath, uuid string) (string, error) {
	uuids, err := ParseUUIDIndex(indexPath)
	if err != nil {
		return "", errors.Trace(err)
	}

	// newer is preferred
	for i := len(uuids) - 1; i >= 0; i-- {
		uuid2, _, err := ParseSuffixForUUID(uuids[i])
		if err != nil {
			return "", errors.Trace(err)
		}
		if uuid2 == uuid {
			return uuids[i], nil
		}
	}

	return "", errors.Errorf("no UUID (with suffix) matched %s found in %s, all UUIDs are %v", uuid, indexPath, uuids)
}

// GetUUIDBySuffix gets UUID from uuids by suffix
func GetUUIDBySuffix(uuids []string, suffix string) string {
	suffix2 := fmt.Sprintf("%s%s", uuidIndexSeparator, suffix)
	for _, uuid := range uuids {
		if strings.HasSuffix(uuid, suffix2) {
			return uuid
		}
	}
	return ""
}

// GenFakeRotateEvent generates a fake ROTATE_EVENT without checksum
// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L855
func GenFakeRotateEvent(nextLogName string, logPos uint64, serverID uint32) (*replication.BinlogEvent, error) {
	headerLen := replication.EventHeaderSize
	bodyLen := 8 + len(nextLogName)
	eventSize := headerLen + bodyLen

	rawData := make([]byte, eventSize)

	// header
	binary.LittleEndian.PutUint32(rawData, 0)                         // timestamp
	rawData[4] = byte(replication.ROTATE_EVENT)                       // event type
	binary.LittleEndian.PutUint32(rawData[4+1:], serverID)            // server ID
	binary.LittleEndian.PutUint32(rawData[4+1+4:], uint32(eventSize)) // event size
	binary.LittleEndian.PutUint32(rawData[4+1+4+4:], 0)               // log pos, always 0
	binary.LittleEndian.PutUint16(rawData[4+1+4+4+4:], 0x20)          // flags, LOG_EVENT_ARTIFICIAL_F

	// body
	binary.LittleEndian.PutUint64(rawData[headerLen:], logPos)
	copy(rawData[headerLen+8:], []byte(nextLogName))

	// decode header
	h := &replication.EventHeader{}
	err := h.Decode(rawData)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// decode body
	e := &replication.RotateEvent{}
	err = e.Decode(rawData[headerLen:])
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}, nil
}
