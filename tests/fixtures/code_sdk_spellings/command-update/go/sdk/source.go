// Copyright (c) 2026 Kenneth Stott
// Canary: 3991f453-5764-4eac-811c-501a368015c3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import provisa "provisa.dev/sdk/go"

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Changes struct {
	Name *string `json:"name"`
}

// UpdateRows is the update of rows.
func UpdateRows(keys []int64, changes Changes) int64 { return 0 }

var _ = provisa.Command(UpdateRows, provisa.Update("rows"))
