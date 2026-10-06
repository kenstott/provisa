// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8d4b63-7a10-4c95-be27-91c3e5a0d748
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a Data Profiler's run defaults — sample size, the low-cardinality threshold and the
// drift window, season and thresholds, the latter defaulted by the form — travel in the source's
// mapping.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { ProfilerFormSection } from "../ProfilerFormSection";
import {
  PROFILER_DEFAULTS,
  profilerFieldsFromMapping,
  profilerMappingJson,
} from "../profilerMapping";

const STORED = {
  cron: "0 * * * *",
  sample_above_cells: 5000,
  low_cardinality_max: 40,
  drift_window: 4,
  drift_season: "weekly",
  drift_distance: 2.5,
  drift_slope: 4,
  drift_ks: 0.1,
  drift_psi: 0.2,
  correlation_max_columns: 10,
  joint_max_distinct: 15,
  category_max_columns: 4,
};

describe("Data Profiler source fields", () => {
  it("defaults every run default for a new profiler", () => {
    const setAuthFields = vi.fn();
    render(
      <ProfilerFormSection authFields={{ cron: "0 3 * * *" }} setAuthFields={setAuthFields} />,
    );
    expect(setAuthFields).toHaveBeenCalledWith({ cron: "0 3 * * *", ...PROFILER_DEFAULTS });
  });

  it("groups the settings into panels, with the schedule open", () => {
    render(
      <ProfilerFormSection
        authFields={{ cron: "0 3 * * *", ...PROFILER_DEFAULTS }}
        setAuthFields={vi.fn()}
      />,
    );
    for (const name of ["Schedule and sampling", "Drift", "Dependence between columns"]) {
      expect(screen.getByRole("button", { name })).toBeInTheDocument();
    }
    expect(screen.getByRole("button", { name: "Schedule and sampling" })).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByRole("button", { name: "Drift" })).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByTestId("profiler-cron-input")).toBeVisible();
  });

  it("offers a field for each drift setting", () => {
    render(
      <ProfilerFormSection
        authFields={{ cron: "0 3 * * *", ...PROFILER_DEFAULTS }}
        setAuthFields={vi.fn()}
      />,
    );
    for (const key of ["drift_season", "drift_window", "drift_distance", "drift_slope"]) {
      expect(screen.getByTestId(`profiler-${key}-input`)).toBeInTheDocument();
    }
    expect(screen.getByTestId("profiler-drift_ks-input")).toHaveValue("0.2");
    expect(screen.getByTestId("profiler-drift_psi-input")).toHaveValue("0.25");
  });

  it("keeps stored settings and round-trips the mapping", () => {
    const setAuthFields = vi.fn();
    const fields = profilerFieldsFromMapping(JSON.stringify(STORED));
    render(<ProfilerFormSection authFields={fields} setAuthFields={setAuthFields} />);
    expect(setAuthFields).not.toHaveBeenCalled();
    expect(JSON.parse(profilerMappingJson(fields))).toEqual(STORED);
    const { sample_above_cells: _, ...whole } = STORED;
    expect(JSON.parse(profilerMappingJson({ ...fields, sample_above_cells: "" }))).toEqual(whole);
  });
});
