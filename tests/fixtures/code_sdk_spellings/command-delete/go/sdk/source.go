// Copyright (c) 2026 Kenneth Stott
// Canary: 429d9d35-7dcd-4d3b-9f81-166f7f92add1
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

// DeleteRows is the delete of rows.
func DeleteRows(keys []int64) int64 { return 0 }

var _ = provisa.Command(DeleteRows, provisa.Delete("rows"))
