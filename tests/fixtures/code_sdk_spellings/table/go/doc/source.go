// Copyright (c) 2026 Kenneth Stott
// Canary: e3be6cfc-06f0-40cc-a3dd-6de2bd9f0007
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

// Rows is every row.
//
// @table
func Rows() []Row { return nil }
