// Copyright (c) 2026 Kenneth Stott
// Canary: 74dbe4d8-8cee-4fdf-801c-0fa37bfd1721
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Roles page's "Add role" opens a menu (REQ-2078): a granted role, or one derived from an
// attribute. A test of the granted-role form opens it here, with the description every role needs
// (REQ-2080) filled in.

import { fireEvent, screen, waitFor } from "./render";

export async function openNewRole(description = "What the role is for.") {
  fireEvent.click(await screen.findByTestId("toggle-role-form"));
  fireEvent.click(await screen.findByTestId("add-granted-role"));
  // The menu hands focus back to its button as it closes; a field clicked before then loses it.
  await waitFor(() => {
    if (screen.queryByTestId("add-granted-role")) throw new Error("menu still open");
  });
  fireEvent.change(await screen.findByTestId("role-description-input"), {
    target: { value: description },
  });
}

/** The derived roles read on the Roles page: none, unless a test declares some. */
export const NO_DERIVED_ROLES = { roles: [], source: "deployment" as const };
