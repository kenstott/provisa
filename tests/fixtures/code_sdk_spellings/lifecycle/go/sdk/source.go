// Copyright (c) 2026 Kenneth Stott
// Canary: 3c0596fd-5672-4868-af2d-0ee89df6c10e
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

// Start opens the client.
func Start(values map[string]string) error { return nil }

// Stop closes the client.
func Stop() {}

func Status() string { return "ok" }

func Rows() []Row { return nil }

var (
	_ = provisa.OnStart(Start)
	_ = provisa.OnStop(Stop)
	_ = provisa.Health(Status)
	_ = provisa.Table[provisa.NoInputs, Row](Rows)
)
