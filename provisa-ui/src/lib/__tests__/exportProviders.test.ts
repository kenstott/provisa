// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, expect, it } from "vitest";
import i18n from "../../i18n";
import { exportProviderName } from "../exportProviders";

describe("exportProviderName", () => {
  const t = i18n.getFixedT("en");

  it("names a beta provider, from the generated list, with its beta label", () => {
    expect(exportProviderName("atlan", "Atlan", t)).toBe("Atlan (beta)");
  });

  it("names a proven provider as it is", () => {
    expect(exportProviderName("atlas", "Apache Atlas", t)).toBe("Apache Atlas");
  });

  it("reads the beta set it is given", () => {
    expect(exportProviderName("atlas", "atlas", t, new Set(["atlas"]))).toBe("atlas (beta)");
    expect(exportProviderName("atlan", "atlan", t, new Set())).toBe("atlan");
  });

  it("fills the copy that names Atlan with its label", () => {
    const atlan = exportProviderName("atlan", "Atlan", t);
    expect(t("tableEditForm.dataProductDesc", { atlan })).toContain("Atlan (beta)");
    expect(t("tour.steps.stepPublish.description", { atlan })).toContain("Atlan (beta)");
  });
});
