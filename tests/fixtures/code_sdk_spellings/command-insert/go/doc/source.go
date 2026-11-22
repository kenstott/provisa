// Copyright (c) 2026 Kenneth Stott
// Canary: 4fdef474-3a9b-44b3-a0f7-ba1c0c08dbc5
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

// InsertRows is the insert of rows.
//
// @command
// @insert rows
func InsertRows(rows []Row) int64 { return 0 }
