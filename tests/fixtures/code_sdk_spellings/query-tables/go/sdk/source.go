// Copyright (c) 2026 Kenneth Stott
// Canary: 43ab61fb-483d-406b-8b17-c67e7b015f2e
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

type Match struct {
	LeftID  int64 `json:"left_id"`
	RightID int64 `json:"right_id"`
}

// Pairs is the rows of two tables that belong together.
func Pairs(left provisa.TableRef[Row], right provisa.TableRef[Row]) ([]Match, error) {
	return nil, nil
}

var _ = provisa.Query(Pairs)
