// Copyright (c) 2026 Kenneth Stott
// Canary: cf95f072-b5e4-43c3-b509-3a835211ae78
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

// DeleteRows is the delete of rows.
//
// @command
// @delete rows
func DeleteRows(keys []int64) int64 { return 0 }
