// Copyright (c) 2026 Kenneth Stott
// Canary: 8664d33f-941f-4cae-9896-c6e9d09cb016
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Start opens the client.
//
// @on-start
func Start(values map[string]string) error { return nil }

// Stop closes the client.
//
// @on-stop
func Stop() {}

// @health
func Status() string { return "ok" }

// @table
func Rows() []Row { return nil }
