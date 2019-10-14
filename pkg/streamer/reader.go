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

package streamer

import (
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/go-mysql-elasticsearch/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	"github.com/siddontang/go-mysql-elasticsearch/pkg/utils"
)

// errors used by reader
var (
	ErrReaderRunning          = errors.New("binlog reader is already running")
	ErrBinlogFileNotSpecified = errors.New("binlog file must be specified")

	// in order to differ binlog pos from multi (switched) masters, we added a UUID-suffix field into binlogPos.Name
	// and we also need support: with UUIDSuffix's pos should always > without UUIDSuffix's pos, so we can update from @without to @with automatically
	// conversion: originalPos.NamePrefix + posUUIDSuffixSeparator + UUIDSuffix + baseSeqSeparator + originalPos.NameSuffix => convertedPos.Name
	// UUIDSuffix is the suffix of sub relay directory name, and when new sub directory created, UUIDSuffix is incremented
	// eg. mysql-bin.000003 in c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002 => mysql-bin|000002.000003
	// where `000002` in `c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002` is the UUIDSuffix
	posUUIDSuffixSeparator = "|"

	// polling interval for watcher
	watcherInterval = 100 * time.Millisecond
)

// BinlogReaderConfig is the configuration for BinlogReader
type BinlogReaderConfig struct {
	RelayDir string
	Timezone *time.Location
}

// BinlogReader is a binlog reader.
type BinlogReader struct {
	cfg    *BinlogReaderConfig
	parser *replication.BinlogParser

	indexPath string   // relay server-uuid index file path
	uuids     []string // master UUIDs (relay sub dir)

	latestServerID uint32 // latest server ID, got from relay log

	running bool
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewBinlogReader creates a new BinlogReader
func NewBinlogReader(cfg *BinlogReaderConfig) *BinlogReader {
	ctx, cancel := context.WithCancel(context.Background())
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	// useDecimal must set true.  ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
	parser.SetUseDecimal(true)
	if cfg.Timezone != nil {
		parser.SetTimestampStringLocation(cfg.Timezone)
	}
	return &BinlogReader{
		cfg:       cfg,
		parser:    parser,
		indexPath: path.Join(cfg.RelayDir, utils.UUIDIndexFilename),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// StartSync start syncon
// TODO:  thread-safe?
func (r *BinlogReader) StartSync(pos mysql.Position) (Streamer, error) {
	if pos.Name == "" {
		return nil, ErrBinlogFileNotSpecified
	}
	if r.running {
		return nil, ErrReaderRunning
	}

	// load and update UUID list
	err := r.updateUUIDs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.latestServerID = 0
	r.running = true
	s := newLocalStreamer()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		log.Infof("[streamer] start read from pos %v", pos)
		err = r.parseRelay(r.ctx, s, pos)
		if errors.Cause(err) == r.ctx.Err() {
			log.Infof("[streamer] parse relay finished because %v", r.ctx.Err())
		} else if err != nil {
			s.closeWithError(err)
			log.Errorf("[streamer] parse relay stopped because %v", errors.ErrorStack(err))
		}
	}()

	return s, nil
}

// parseRelay parses relay root directory, it support master-slave switch (switching to next sub directory)
func (r *BinlogReader) parseRelay(ctx context.Context, s *LocalStreamer, pos mysql.Position) error {
	var (
		needSwitch     bool
		nextUUID       string
		nextBinlogName string
		err            error
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		needSwitch, nextUUID, nextBinlogName, err = r.parseDirAsPossible(ctx, s, pos)
		if err != nil {
			return errors.Trace(err)
		}
		if !needSwitch {
			return errors.NotSupportedf("parse for previous sub relay directory finished, but no next sub directory need to switch")
		}

		_, suffixInt, err2 := utils.ParseSuffixForUUID(nextUUID)
		if err2 != nil {
			return errors.Annotatef(err2, "parse suffix for UUID %s", nextUUID)
		}
		uuidSuffix := utils.SuffixIntToStr(suffixInt)

		parsed, err2 := parseBinlogFile(nextBinlogName)
		if err2 != nil {
			return errors.Annotatef(err2, "parse binlog file name %s", nextBinlogName)
		}

		// update pos, so can switch to next sub directory
		pos.Name = constructBinlogName(parsed, uuidSuffix)
		pos.Pos = 4 // start from pos 4 for next sub directory / file
		log.Infof("[streamer] switching to next realy sub directory with UUID %s and pos %s", nextUUID, pos)
	}
}

// parseDirAsPossible parses relay sub directory as far as possible
func (r *BinlogReader) parseDirAsPossible(ctx context.Context, s *LocalStreamer, pos mysql.Position) (needSwitch bool, nextUUID string, nextBinlogName string, err error) {
	currentUUID, _, realPos, err := ExtractPos(pos, r.uuids)
	if err != nil {
		return false, "", "", errors.Annotatef(err, "parse relay dir with pos %v", pos)
	}
	pos = realPos         // use realPos to do syncing
	var firstParse = true // the first parse time for the relay log file
	var dir = path.Join(r.cfg.RelayDir, currentUUID)
	log.Infof("[streamer] start to parse relay log files in sub directory %s with pos %s", dir, pos)

	for {
		select {
		case <-ctx.Done():
			return false, "", "", ctx.Err()
		default:
		}
		files, err := CollectBinlogFilesCmp(dir, pos.Name, FileCmpBiggerEqual)
		if err != nil {
			return false, "", "", errors.Annotatef(err, "parse relay dir %s with pos %s", dir, pos)
		} else if len(files) == 0 {
			return false, "", "", errors.Errorf("no relay log files in dir %s match pos %s", dir, pos)
		}

		log.Debugf("[streamer] start read relay log files %v in directory %s with pos %s", files, dir, pos)

		var (
			latestPos  int64
			latestName string
			offset     = int64(pos.Pos)
		)
		for i, relayLogFile := range files {
			select {
			case <-ctx.Done():
				return false, "", "", ctx.Err()
			default:
			}
			if i == 0 {
				if !strings.HasSuffix(relayLogFile, pos.Name) {
					return false, "", "", errors.Errorf("the first relay log %s not match the start pos %v", relayLogFile, pos)
				}
			} else {
				offset = 4        // for other relay log file, start parse from 4
				firstParse = true // new relay log file need to parse
			}
			needSwitch, latestPos, nextUUID, nextBinlogName, err = r.parseFileAsPossible(ctx, s, relayLogFile, offset, dir, firstParse, currentUUID, i == len(files)-1)
			firstParse = false // already parsed
			if err != nil {
				return false, "", "", errors.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, offset, dir)
			}
			if needSwitch {
				// need switch to next relay sub directory
				return true, nextUUID, nextBinlogName, nil
			}
			latestName = relayLogFile // record the latest file name
		}

		// update pos, so can re-collect files from the latest file and re start parse from latest pos
		pos.Pos = uint32(latestPos)
		pos.Name = latestName
	}
}

// parseFileAsPossible parses single relay log file as far as possible
func (r *BinlogReader) parseFileAsPossible(ctx context.Context, s *LocalStreamer, relayLogFile string, offset int64, relayLogDir string, firstParse bool, currentUUID string, possibleLast bool) (needSwitch bool, latestPos int64, nextUUID string, nextBinlogName string, err error) {
	var (
		needReParse bool
	)
	latestPos = offset
	log.Debugf("[streamer] start to parse relay log file %s from offset %d in dir %s", relayLogFile, latestPos, relayLogDir)

	for {
		select {
		case <-ctx.Done():
			return false, 0, "", "", ctx.Err()
		default:
		}
		needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(ctx, s, relayLogFile, latestPos, relayLogDir, firstParse, currentUUID, possibleLast)
		firstParse = false // set to false to handle the `continue` below
		if err != nil {
			return false, 0, "", "", errors.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, latestPos, relayLogDir)
		}
		if needReParse {
			log.Debugf("[streamer] continue to re-parse relay log file %s in dir %s", relayLogFile, relayLogDir)
			continue // should continue to parse this file
		}
		return needSwitch, latestPos, nextUUID, nextBinlogName, nil
	}
}

// parseFile parses single relay log file from specified offset
func (r *BinlogReader) parseFile(ctx context.Context, s *LocalStreamer, relayLogFile string, offset int64, relayLogDir string, firstParse bool, currentUUID string, possibleLast bool) (needSwitch, needReParse bool, latestPos int64, nextUUID string, nextBinlogName string, err error) {
	_, suffixInt, err := utils.ParseSuffixForUUID(currentUUID)
	if err != nil {
		return false, false, 0, "", "", errors.Trace(err)
	}

	uuidSuffix := utils.SuffixIntToStr(suffixInt) // current UUID's suffix, which will be added to binlog name
	latestPos = offset                            // set to argument passed in

	onEventFunc := func(e *replication.BinlogEvent) error {
		log.Debugf("[streamer] read event %+v", e.Header)
		if e.Header.Flags&0x0040 != 0 {
			// now LOG_EVENT_RELAY_LOG_F is only used for events which used to fill the gap in relay log file when switching the master server
			log.Debugf("skip event %+v created by relay writer", e.Header)
			return nil
		}

		r.latestServerID = e.Header.ServerID // record server_id

		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			// ignore FORMAT_DESCRIPTION event, because go-mysql will send this fake event
		case replication.ROTATE_EVENT:
			// add master UUID suffix to pos.Name
			env := e.Event.(*replication.RotateEvent)
			parsed, _ := parseBinlogFile(string(env.NextLogName))
			nameWithSuffix := constructBinlogName(parsed, uuidSuffix)
			env.NextLogName = []byte(nameWithSuffix)

			if e.Header.Timestamp != 0 && e.Header.LogPos != 0 {
				// not fake rotate event, update file pos
				latestPos = int64(e.Header.LogPos)
			} else {
				log.Debugf("[streamer] skip fake rotate event %+v", e.Header)
			}

			currentPos := mysql.Position{
				Name: string(env.NextLogName),
				Pos:  uint32(env.Position),
			}
			log.Infof("[streamer] rotate binlog to %v", currentPos)
		default:
			// update file pos
			latestPos = int64(e.Header.LogPos)
		}

		select {
		case s.ch <- e:
		case <-ctx.Done():
		}
		return nil
	}

	fullPath := filepath.Join(relayLogDir, relayLogFile)

	if firstParse {
		// if the file is the first time to parse, send a fake ROTATE_EVENT before parse binlog file
		// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L248
		e, err2 := utils.GenFakeRotateEvent(relayLogFile, uint64(offset), r.latestServerID)
		if err2 != nil {
			return false, false, 0, "", "", errors.Annotatef(err2, "generate fake RotateEvent for (%s: %d)", relayLogFile, offset)
		}
		err2 = onEventFunc(e)
		if err2 != nil {
			return false, false, 0, "", "", errors.Annotatef(err2, "send event %+v", e.Header)
		}
		log.Infof("[streamer] start parse relay log file %s from offset %d", fullPath, offset)
	} else {
		log.Debugf("[streamer] start parse relay log file %s from offset %d", fullPath, offset)
	}

	err = r.parser.ParseFile(fullPath, offset, onEventFunc)
	if possibleLast && err != nil && strings.Contains(err.Error(), "err EOF") {
		// NOTE: go-mysql returned err not includes caused err, but as message, ref: parser.go `parseSingleEvent`
		log.Warnf("[streamer] parse relay log file %s from offset %d got EOF %s", fullPath, offset, errors.ErrorStack(err))
	} else if err != nil {
		log.Errorf("[streamer] parse relay log file %s from offset %d error %s", fullPath, offset, errors.ErrorStack(err))
		return false, false, 0, "", "", errors.Annotatef(err, "relay log file %s", fullPath)
	}
	log.Debugf("[streamer] parse relay log file %s return with offset %d", fullPath, latestPos)

	if !possibleLast {
		// there are more relay log files in current sub directory, continue to re-collect them
		log.Infof("[streamer] more relay log files need to parse in dir %s", relayLogDir)
		return false, false, latestPos, "", "", nil
	}

	needSwitch, needReParse, nextUUID, nextBinlogName, err = r.needSwitchSubDir(currentUUID, fullPath, int64(latestPos))
	if err != nil {
		return false, false, 0, "", "", errors.Trace(err)
	} else if needReParse {
		// need to re-parse the current relay log file
		return false, true, latestPos, "", "", nil
	} else if needSwitch {
		// need to switch to next relay sub directory
		return true, false, 0, nextUUID, nextBinlogName, nil
	}

	updatedPath, err := relaySubDirUpdated(ctx, watcherInterval, relayLogDir, fullPath, relayLogFile, int64(latestPos))
	if err != nil {
		return false, false, 0, "", "", errors.Trace(err)
	}

	if strings.HasSuffix(updatedPath, relayLogFile) {
		// current relay log file updated, need to re-parse it
		return false, true, latestPos, "", "", nil
	}

	// need parse next relay log file or re-collect files
	return false, false, latestPos, "", "", nil
}

// needSwitchSubDir checks whether the reader need switch to next relay sub directory
func (r *BinlogReader) needSwitchSubDir(currentUUID string, latestFilePath string, latestFileSize int64) (needSwitch, needReParse bool, nextUUID string, nextBinlogName string, err error) {
	nextUUID, _ = r.getNextUUID(currentUUID)
	if len(nextUUID) == 0 {
		// no next sub dir exists, not need to switch
		return false, false, "", "", nil
	}

	// try get the first binlog file in next sub directory
	nextBinlogName, err = getFirstBinlogName(r.cfg.RelayDir, nextUUID)
	if err != nil {
		// NOTE: current we can not handle `errors.IsNotFound(err)` easily
		// because creating sub directory and writing relay log file are not atomic
		// so we let user to pause syncing before switching relay's master server
		return false, false, "", "", errors.Trace(err)
	}

	// check the latest relay log file whether updated when checking next sub directory
	cmp, err := fileSizeUpdated(latestFilePath, latestFileSize)
	if err != nil {
		return false, false, "", "", errors.Trace(err)
	} else if cmp < 0 {
		return false, false, "", "", errors.Errorf("file size of relay log %s become smaller", latestFilePath)
	} else if cmp > 0 {
		// the latest relay log file already updated, need to parse from it again (not need to switch sub directory)
		return false, true, "", "", nil
	}

	// need to switch to next sub directory
	return true, false, nextUUID, nextBinlogName, nil
}

// updateUUIDs re-parses UUID index file and updates UUID list
func (r *BinlogReader) updateUUIDs() error {
	uuids, err := utils.ParseUUIDIndex(r.indexPath)
	if err != nil {
		return errors.Annotatef(err, "index file path %s", r.indexPath)
	}
	oldUUIDs := r.uuids
	r.uuids = uuids
	log.Infof("[streamer] update relay UUIDs from %v to %v", oldUUIDs, uuids)
	return nil
}

func (r *BinlogReader) getNextUUID(uuid string) (string, string) {
	for i := len(r.uuids) - 2; i >= 0; i-- {
		if r.uuids[i] == uuid {
			nextUUID := r.uuids[i+1]
			_, suffixInt, _ := utils.ParseSuffixForUUID(nextUUID)
			return nextUUID, utils.SuffixIntToStr(suffixInt)
		}
	}
	return "", ""
}

// Close closes BinlogReader.
func (r *BinlogReader) Close() error {
	log.Info("[streamer] binlog reader closing")
	r.running = false
	r.cancel()
	r.parser.Stop()
	r.wg.Wait()
	log.Info("[streamer] binlog reader closed")
	return nil
}
