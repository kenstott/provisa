// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8d4b63-7a10-4c95-be27-91c3e5a0d748
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a Data Profiler's run defaults — sample size and the low-cardinality threshold, the
// latter defaulted by the form — travel in the source's mapping.

import { describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";
import { ProfilerFormSection } from "../ProfilerFormSection";
import {
  DEFAULT_LOW_CARDINALITY_MAX,
  profilerFieldsFromMapping,
  profilerMappingJson,
} from "../profilerMapping";

describe("Data Profiler source fields", () => {
  it("defaults the low-cardinality threshold for a new profiler", () => {
    const setAuthFields = vi.fn();
    render(
      <ProfilerFormSection authFields={{ cron: "0 3 * * *" }} setAuthFields={setAuthFields} />,
    );
    expect(setAuthFields).toHaveBeenCalledWith({
      cron: "0 3 * * *",
      low_cardinality_max: DEFAULT_LOW_CARDINALITY_MAX,
    });
  });

  it("keeps a stored threshold and round-trips the mapping", () => {
    const setAuthFields = vi.fn();
    const fields = profilerFieldsFromMapping(
      JSON.stringify({ cron: "0 * * * *", sample_above_rows: 5000, low_cardinality_max: 40 }),
    );
    render(<ProfilerFormSection authFields={fields} setAuthFields={setAuthFields} />);
    expect(setAuthFields).not.toHaveBeenCalled();
    expect(JSON.parse(profilerMappingJson(fields))).toEqual({
      cron: "0 * * * *",
      sample_above_rows: 5000,
      low_cardinality_max: 40,
    });
    expect(JSON.parse(profilerMappingJson({ ...fields, sample_above_rows: "" }))).toEqual({
      cron: "0 * * * *",
      low_cardinality_max: 40,
    });
  });
});
