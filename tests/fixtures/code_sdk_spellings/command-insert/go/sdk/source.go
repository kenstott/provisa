// Copyright (c) 2026 Kenneth Stott
// Canary: a589b3a5-596e-4731-a7bc-260e1156e2b7
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

// InsertRows is the insert of rows.
func InsertRows(rows []Row) int64 { return 0 }

var _ = provisa.Command(InsertRows, provisa.Insert("rows"))
