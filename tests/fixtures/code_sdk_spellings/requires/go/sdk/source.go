// Copyright (c) 2026 Kenneth Stott
// Canary: d685c552-3316-44cc-a7bd-cbb59365faa3
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

// Rows is the rows from the API.
func Rows() []Row { return nil }

var _ = provisa.Table[provisa.NoInputs, Row](
	Rows, provisa.Requires("API_KEY", provisa.Secret), provisa.Requires("REGION"),
)
