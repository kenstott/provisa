// Copyright (c) 2026 Kenneth Stott
// Canary: ae8bb371-6c53-4f83-9e16-6b23b0aa5273
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

// Cancel cancels one order.
func Cancel(orderID int64) (int64, error) { return 1, nil }

var _ = provisa.Command(Cancel)
