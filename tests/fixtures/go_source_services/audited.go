// Copyright (c) 2026 Kenneth Stott
// Canary: 493829fd-b775-40b4-813b-8a7fc8e95772
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Package audited is a Go source that uses Provisa's services (REQ-2106). Its go.mod does not
// mention provisa_services: the module is always present where the code runs.
package audited

import (
	"context"

	ps "provisa_services"
)

type AuditedInputs struct {
	Name string `json:"name"`
}

type Audited struct {
	Name string `json:"name"`
}

// Recorded writes an event to the org's trail for the request it serves.
//
// @table
func Recorded(ctx context.Context, in AuditedInputs) ([]Audited, error) {
	if err := ps.Audit(ctx, "go.read", map[string]any{"name": in.Name}); err != nil {
		return nil, err
	}
	return []Audited{{Name: in.Name}}, nil
}
