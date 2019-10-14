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
	"encoding/base64"

	"github.com/siddontang/go-mysql-elasticsearch/pkg/encrypt"
	"github.com/pingcap/errors"
)

// Encrypt tries to encrypt plaintext to base64 encoded ciphertext
func Encrypt(plaintext string) (string, error) {
	ciphertext, err := encrypt.Encrypt([]byte(plaintext))
	if err != nil {
		return "", errors.Trace(err)
	}

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt tries to decrypt base64 encoded ciphertext to plaintext
func Decrypt(ciphertextB64 string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", errors.Trace(err)
	}

	plaintext, err := encrypt.Decrypt(ciphertext)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(plaintext), nil
}
