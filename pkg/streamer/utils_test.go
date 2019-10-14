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
	"context"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testStreamerSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testStreamerSuite struct {
}

func (s *testStreamerSuite) TestRelaySubDirUpdated(c *C) {
	var (
		relayFiles = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
			"mysql-bin.000003",
			"mysql-bin.000004",
		}
		relayPaths      = make([]string, len(relayFiles))
		data            = []byte("meaningless file content")
		size            = int64(len(data))
		watcherInterval = 100 * time.Millisecond
	)

	// create relay log dir
	subDir, err := ioutil.TempDir("", "test_relay_sub_dir_updated")
	c.Assert(err, IsNil)
	defer os.RemoveAll(subDir)

	// join the path
	for i, rf := range relayFiles {
		relayPaths[i] = path.Join(subDir, rf)
	}

	// create first file
	f, err := os.Create(relayPaths[0])
	c.Assert(err, IsNil)
	f.Close()

	// 1. watcher, for file updated
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		latestFileSize := int64(0)
		up, err2 := relaySubDirUpdated(context.Background(), watcherInterval, subDir, relayPaths[0], relayFiles[0], latestFileSize)
		c.Assert(err2, IsNil)
		c.Assert(up, Equals, relayPaths[0])
	}()

	// update file
	f, err = os.OpenFile(relayPaths[0], os.O_WRONLY|os.O_APPEND, 0666)
	c.Assert(err, IsNil)
	f.Write(data)
	f.Close()
	wg.Wait()

	// 2. create and update file
	f, err = os.Create(relayPaths[1])
	c.Assert(err, IsNil)
	f.Write(data)
	f.Close()

	// watcher, but file size has changed
	wg.Add(1)
	go func() {
		defer wg.Done()
		latestFileSize := size - 1 // less than file size
		up, err2 := relaySubDirUpdated(context.Background(), watcherInterval, subDir, relayPaths[1], relayFiles[1], latestFileSize)
		c.Assert(err2, IsNil)
		c.Assert(up, Equals, relayPaths[1])
	}()
	wg.Wait()

	// 3. watcher, for file created
	wg.Add(1)
	go func() {
		defer wg.Done()
		latstFileSize := size
		up, err2 := relaySubDirUpdated(context.Background(), watcherInterval, subDir, relayPaths[1], relayFiles[1], latstFileSize)
		c.Assert(err2, IsNil)
		c.Assert(up, Equals, relayPaths[2])
	}()

	// create file
	f, err = os.Create(relayPaths[2])
	c.Assert(err, IsNil)
	f.Close()
	wg.Wait()

	// 4. create file
	f, err = os.Create(relayPaths[3])
	c.Assert(err, IsNil)
	f.Close()

	// watcher, for newer file already generated
	wg.Add(1)
	go func() {
		defer wg.Done()
		latstFileSize := int64(0)
		up, err := relaySubDirUpdated(context.Background(), watcherInterval, subDir, relayPaths[2], relayFiles[2], latstFileSize)
		c.Assert(err, IsNil)
		c.Assert(up, Equals, relayPaths[3])
	}()
	wg.Wait()
}

func (s *testStreamerSuite) TestFileSizeUpdatedError(c *C) {
	var (
		relayFile       = "mysql-bin.000001"
		data            = []byte("meaningless file content")
		watcherInterval = 100 * time.Millisecond
	)

	// create relay log dir
	subDir, err := ioutil.TempDir("", "test_file_size_updated_error")
	c.Assert(err, IsNil)
	defer os.RemoveAll(subDir)

	// create relay log
	relayPath := path.Join(subDir, relayFile)
	err = ioutil.WriteFile(relayPath, []byte(data), 0644)
	c.Assert(err, IsNil)

	// watcher, for file size decrease
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		latestFileSize := int64(len(data))
		_, err2 := relaySubDirUpdated(context.Background(), watcherInterval, subDir, relayPath, relayFile, latestFileSize)
		c.Assert(err2, NotNil)
	}()

	// truncate relay log
	err = ioutil.WriteFile(relayPath, []byte(""), 0644)
	c.Assert(err, IsNil)
	wg.Wait()

	// watcher, for file not found
	wg.Add(1)
	go func() {
		defer wg.Done()
		latestFileSize := int64(0)
		_, err2 := relaySubDirUpdated(context.Background(), watcherInterval, subDir, relayPath, relayFile, latestFileSize)
		c.Assert(err2, NotNil)
	}()

	err = os.Remove(relayPath)
	c.Assert(err, IsNil)
	wg.Wait()
}
