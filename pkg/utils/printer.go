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
	"fmt"

		"github.com/siddontang/go-mysql-elasticsearch/pkg/log"
)

// Version information.
var (
	ReleaseVersion = "None"
	BuildTS        = "None"
	GitHash        = "None"
	GitBranch      = "None"
	GoVersion      = "None"
)

// GetRawInfo do what its name tells
func GetRawInfo() string {
	var info string
	info += fmt.Sprintf("Release Version: %s\n", ReleaseVersion)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s\n", GitBranch)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", GoVersion)
	return info
}

// PrintInfo prints some information of the app, like git hash, binary build time, etc.
func PrintInfo(app string, callback func()) {
	oriLevel := log.GetLogLevelAsString()
	log.SetLevelByString("info")
	printInfo(app)
	callback()
	log.SetLevelByString(oriLevel)
}

func printInfo(app string) {
	log.Infof("Welcome to %s", app)
	log.Infof("Release Version: %s", ReleaseVersion)
	log.Infof("Git Commit Hash: %s", GitHash)
	log.Infof("Git Branch: %s", GitBranch)
	log.Infof("UTC Build Time: %s", BuildTS)
	log.Infof("Go Version: %s", GoVersion)
}

// PrintInfo2 print app's info to stdout
func PrintInfo2(app string) {
	fmt.Printf("Welcome to %s\n", app)
	fmt.Printf("Release Version: %s\n", ReleaseVersion)
	fmt.Printf("Git Commit Hash: %s\n", GitHash)
	fmt.Printf("Git Branch: %s\n", GitBranch)
	fmt.Printf("UTC Build Time: %s\n", BuildTS)
	fmt.Printf("Go Version: %s\n", GoVersion)
}
