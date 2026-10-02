// Copyright (c) 2026 Kenneth Stott
// Canary: fccfd1d1-10e9-4ddc-8b09-52b277ee337f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An object a config file declares is marked in its list, and a change to it that went through
// tells the person what the next load of the config does (REQ-1919).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { notifications } from "@mantine/notifications";
import { render, screen } from "../../test-utils/render";
import { OriginBadge } from "../OriginBadge";
import { announceWarnings, warningsIn } from "../../lib/mutationWarnings";

const EDITED = {
  code: "origin.config_object_edited",
  message: "role 'seller' is declared in the config: the next load of the config re-applies what the file says",
  params: { kind: "role", name: "seller" },
};

describe("OriginBadge", () => {
  it("marks an object a config declares", () => {
    render(<OriginBadge origin="config" />);
    expect(screen.getByTestId("origin-badge")).toHaveTextContent("declared in config");
  });

  it.each(["admin", "seed"] as const)("shows nothing for an object of origin %s", (origin) => {
    render(<OriginBadge origin={origin} />);
    expect(screen.queryByTestId("origin-badge")).toBeNull();
  });
});

describe("warningsIn", () => {
  it("reads the warnings of every result in a GraphQL response", () => {
    const data = {
      createRole: { success: true, message: "", warnings: [EDITED] },
      deleteDomain: { success: true, message: "", warnings: [] },
    };
    expect(warningsIn(data)).toEqual([EDITED]);
  });

  it("reads the warnings of a REST answer", () => {
    expect(warningsIn({ deleted: "seller", warnings: [EDITED] })).toEqual([EDITED]);
  });

  it("is empty for a response that carries none", () => {
    expect(warningsIn({ roles: [{ id: "seller" }] })).toEqual([]);
    expect(warningsIn(null)).toEqual([]);
    expect(warningsIn(undefined)).toEqual([]);
  });
});

describe("announceWarnings", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("shows each warning in the reader's language and leaves it up until dismissed", () => {
    const show = vi.spyOn(notifications, "show").mockImplementation(() => "id");
    announceWarnings({ createRole: { success: true, message: "", warnings: [EDITED] } });
    expect(show).toHaveBeenCalledTimes(1);
    expect(show).toHaveBeenCalledWith({
      color: "yellow",
      autoClose: false,
      message:
        "seller is declared in the config. The change is saved; the next load of the config re-applies what the file says.",
    });
  });

  it("shows nothing when the change carries no warning", () => {
    const show = vi.spyOn(notifications, "show").mockImplementation(() => "id");
    announceWarnings({ createRole: { success: true, message: "", warnings: [] } });
    expect(show).not.toHaveBeenCalled();
  });
});
