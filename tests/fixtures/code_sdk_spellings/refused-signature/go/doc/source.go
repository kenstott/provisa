// Copyright (c) 2026 Kenneth Stott
// Canary: 86515eaf-cc7e-4581-861b-03d258534cb0
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
func Rows(in Inputs, more Inputs) []Row { return nil }
