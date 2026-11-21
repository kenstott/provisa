// Copyright (c) 2026 Kenneth Stott
// Canary: 1449403e-efb2-441d-8a1d-a2f63212bb40
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

type Inputs struct {
	Region *string `json:"region"`
}

// @table
// @loader
func Rows(in Inputs) []Row { return nil }
