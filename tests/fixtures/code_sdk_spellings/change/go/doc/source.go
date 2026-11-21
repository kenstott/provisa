// Copyright (c) 2026 Kenneth Stott
// Canary: 6ca90b9c-7aec-4107-95d5-22365ac8bec2
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

// TableRef is a governed table the code takes; Provisa's reader knows it by its name.
type TableRef[Row any] struct{}

type Changed struct {
	LeftID  int64 `json:"left_id"`
	RightID int64 `json:"right_id"`
	Removed bool  `json:"removed"`
	Seq     int64 `json:"seq"`
}

// PairsChanges is the matches changed since since.
//
// @change table=pairs
// @change key=left_id,right_id
// @change removed=removed
// @change cursor=seq
func PairsChanges(left TableRef[Row], right TableRef[Row], since *int64) ([]Changed, error) {
	return nil, nil
}
