// Copyright (c) 2026 Kenneth Stott
// Canary: 567c9b3f-581b-4f39-839b-082244dc6cd7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Stack } from "@mantine/core";
import { GlossaryTab } from "../../components/admin/GlossaryTab";

/** REQ-2259: the Ontology page of the Context group -- the ontology editor and graph. Its detail
 * panel fills to the bottom of the viewport, so the page joins the flex-height chain `main`
 * provides (`page-sticky-head`). */
export function OntologyPage() {
  return (
    <div className="page page-sticky-head">
      <Stack gap="lg" style={{ flex: 1, minHeight: 0 }}>
        <GlossaryTab />
      </Stack>
    </div>
  );
}
