// Copyright (c) 2026 Kenneth Stott
// Canary: 52ba7409-aa44-49c8-b7ee-b76d446e3971
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

type Changed struct {
	LeftID  int64 `json:"left_id"`
	RightID int64 `json:"right_id"`
	Removed bool  `json:"removed"`
	Seq     int64 `json:"seq"`
}

// PairsChanges is the matches changed since since.
func PairsChanges(
	left provisa.TableRef[Row], right provisa.TableRef[Row], since *int64,
) ([]Changed, error) {
	return nil, nil
}

var _ = provisa.Change(PairsChanges, "pairs", []string{"left_id", "right_id"}, "removed", "seq")
