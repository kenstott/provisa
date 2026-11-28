// Copyright (c) 2026 Kenneth Stott
// Canary: 88ccbcc7-8ac4-49e8-a70d-9c776e53b046
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147: the classification kinds are listed with their test, threshold, whether they are
// sensitive and the mask they offer, and a holder of sensitive_data enables or disables a kind.
// (Amended 2026-11-25) It also adds a custom kind ("+ Kind"), edits and deletes one in its panel,
// changes a shipped kind's share, sensitivity and mask (never its test), and copies a shipped kind
// into a custom one. REQ-2277: a kind's panel is its deep link (?kind=<id>), and a form's required
// fields are checked on Save, never by disabling it.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { expectFilterFindsEveryCell } from "../test-utils/listFilter";

const auth = vi.hoisted(() => ({ capabilities: ["sensitive_data"] as string[] }));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

const api = vi.hoisted(() => ({
  fetchClassificationKinds: vi.fn(),
  createClassificationKind: vi.fn(),
  changeClassificationKind: vi.fn(),
  deleteClassificationKind: vi.fn(),
}));
vi.mock("../api/profiler", () => api);

import { ClassificationKindsTab } from "../components/admin/ClassificationKindsTab";

const SSN = {
  id: "us_ssn",
  label: "US social security number",
  custom: false,
  sensitive: true,
  test: "us_ssn",
  pattern: "(?:\\d{3}-\\d{2}-\\d{4}|\\d{7,9})",
  check: "us_ssn",
  ignore: null,
  families: ["text", "numeric"],
  threshold: 0.9,
  term: "us social security number",
  maskPreset: "ssn_last4",
  enabled: true,
};
const CITY = {
  id: "city",
  label: "World city",
  custom: false,
  sensitive: false,
  test: "reference:cities",
  pattern: null,
  check: null,
  ignore: null,
  families: ["text"],
  threshold: 0.9,
  term: "city",
  maskPreset: null,
  enabled: true,
};
const BADGE = {
  id: "custom.employee_id",
  label: "Employee ID",
  custom: true,
  sensitive: false,
  test: "custom.employee_id",
  pattern: "EMP-\\d{6}",
  check: null,
  ignore: null,
  families: ["text", "numeric"],
  threshold: 0.8,
  term: "employee id",
  maskPreset: null,
  enabled: true,
};
const read = (kinds: object[]) => ({
  kinds,
  checks: ["payment_card", "us_ssn"],
  masks: ["ssn_last4", "card_last4"],
});

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  auth.capabilities = ["sensitive_data"];
  api.fetchClassificationKinds.mockResolvedValue(read([SSN, CITY, BADGE]));
});

describe("ClassificationKindsTab", () => {
  it("lists each kind with its test, threshold and mask; a custom kind by its label", async () => {
    render(<ClassificationKindsTab title="Classification" />);
    const ssn = await screen.findByTestId("classification-kind-us_ssn");
    expect(ssn).toHaveTextContent("US social security number");
    expect(ssn).toHaveTextContent("Sensitive");
    expect(ssn).toHaveTextContent("the area is never 000, 666 or 900 to 999");
    expect(ssn).toHaveTextContent("90%");
    expect(ssn).toHaveTextContent("SSN, last 4");
    expect(screen.getByTestId("classification-kind-city")).toHaveTextContent("None");
    const badge = screen.getByTestId("classification-kind-custom.employee_id");
    expect(badge).toHaveTextContent("Employee ID");
    expect(badge).toHaveTextContent("Custom");
    expect(badge).toHaveTextContent("EMP-\\d{6}");
    expect(badge).toHaveTextContent("80%");
  });

  it("finds each kind by every text its row shows", async () => {
    api.fetchClassificationKinds.mockResolvedValue(
      read([SSN, { ...CITY, threshold: 0.75, enabled: false }, BADGE]),
    );
    render(<ClassificationKindsTab title="Classification" />);
    await screen.findByTestId("classification-kind-city");
    await expectFilterFindsEveryCell(screen.getByTestId("classification-kinds-filter"), () =>
      screen.getByTestId("classification-kinds-list"),
    );
  });

  it("says nothing matches when the filter empties the list", async () => {
    render(<ClassificationKindsTab title="Classification" />);
    await screen.findByTestId("classification-kind-city");
    fireEvent.change(screen.getByTestId("classification-kinds-filter"), {
      target: { value: "no-such-kind" },
    });
    expect(screen.getByTestId("classification-kinds-empty")).toHaveTextContent(
      "No kinds match the filter",
    );
  });

  it("disables a shipped kind, sending only the settings an org may change", async () => {
    api.changeClassificationKind.mockResolvedValue(read([SSN, { ...CITY, enabled: false }, BADGE]));
    api.fetchClassificationKinds
      .mockResolvedValueOnce(read([SSN, CITY, BADGE]))
      .mockResolvedValue(read([SSN, { ...CITY, enabled: false }, BADGE]));
    render(<ClassificationKindsTab title="Classification" />);
    // REQ-2277: the row opens the kind's panel; its Edit opens the form with the switch.
    fireEvent.click(await screen.findByTestId("classification-kind-city"));
    fireEvent.click(screen.getByTestId("classification-kind-edit-city"));
    // A shipped kind's test is not in its form: it is stated, and copied to be changed.
    expect(screen.queryByTestId("classification-kind-pattern")).toBeNull();
    expect(screen.getByTestId("classification-kind-shipped-test")).toHaveTextContent(
      "duplicate it into a custom kind",
    );
    fireEvent.click(screen.getByTestId("classification-kind-enabled-city"));
    fireEvent.click(screen.getByTestId("classification-kind-save"));
    await waitFor(() =>
      expect(api.changeClassificationKind).toHaveBeenCalledWith("city", {
        threshold: 0.9,
        sensitive: false,
        maskPreset: null,
        enabled: false,
      }),
    );
    await waitFor(() =>
      expect(screen.getByTestId("classification-kind-state-city")).toHaveTextContent("Disabled"),
    );
  });

  it("holds the switch while its change is saving", async () => {
    api.changeClassificationKind.mockReturnValue(new Promise(() => {}));
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-city"));
    fireEvent.click(screen.getByTestId("classification-kind-edit-city"));
    fireEvent.click(screen.getByTestId("classification-kind-enabled-city"));
    fireEvent.click(screen.getByTestId("classification-kind-save"));
    await waitFor(() =>
      expect(screen.getByTestId("classification-kind-enabled-city")).toBeDisabled(),
    );
  });

  it("adds a custom kind from + Kind and opens it", async () => {
    const made = { ...BADGE, id: "custom.staff_badge", label: "Staff badge" };
    api.createClassificationKind.mockResolvedValue({ id: made.id, ...read([SSN, CITY, made]) });
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kinds-create"));
    // Required fields are checked on Save, which is never disabled for them.
    const save = screen.getByTestId("classification-kind-save");
    expect(save).not.toBeDisabled();
    fireEvent.click(save);
    const form = screen.getByTestId("classification-kind-form");
    expect(await within(form).findAllByText("Required")).toHaveLength(3);
    expect(api.createClassificationKind).not.toHaveBeenCalled();
    api.fetchClassificationKinds.mockResolvedValue(read([SSN, CITY, made]));
    fireEvent.change(screen.getByTestId("classification-kind-name"), {
      target: { value: "Staff badge" },
    });
    fireEvent.change(screen.getByTestId("classification-kind-pattern"), {
      target: { value: "EMP-\\d{6}" },
    });
    fireEvent.change(screen.getByTestId("classification-kind-term"), {
      target: { value: "employee id" },
    });
    fireEvent.click(screen.getByTestId("classification-kind-save"));
    await waitFor(() =>
      expect(api.createClassificationKind).toHaveBeenCalledWith({
        label: "Staff badge",
        pattern: "EMP-\\d{6}",
        check: null,
        ignore: null,
        term: "employee id",
        threshold: 0.9,
        sensitive: false,
        maskPreset: null,
      }),
    );
    expect(
      await screen.findByTestId("classification-kind-detail-custom.staff_badge"),
    ).toBeVisible();
  });

  it("asks a sensitive kind for its mask before saving", async () => {
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-custom.employee_id"));
    fireEvent.click(screen.getByTestId("classification-kind-edit-custom.employee_id"));
    fireEvent.click(screen.getByTestId("classification-kind-sensitive"));
    fireEvent.click(screen.getByTestId("classification-kind-save"));
    expect(await screen.findByText("A sensitive kind offers a mask")).toBeVisible();
    expect(api.changeClassificationKind).not.toHaveBeenCalled();
  });

  it("copies a shipped kind into a custom one, its test filled in", async () => {
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-us_ssn"));
    fireEvent.click(screen.getByTestId("classification-kind-duplicate-us_ssn"));
    expect(screen.getByTestId("classification-kind-name")).toHaveValue(
      "US social security number (copy)",
    );
    expect(screen.getByTestId("classification-kind-pattern")).toHaveValue(SSN.pattern);
    expect(screen.getByTestId("classification-kind-term")).toHaveValue(SSN.term);
    // A reference kind (a shipped list) has no pattern to copy.
    fireEvent.click(screen.getByTestId("classification-kind-city"));
    expect(screen.queryByTestId("classification-kind-duplicate-city")).toBeNull();
    // A shipped kind is never deleted; a custom one is.
    expect(screen.queryByTestId("classification-kind-delete-city")).toBeNull();
  });

  it("deletes a custom kind once confirmed", async () => {
    api.deleteClassificationKind.mockResolvedValue(read([SSN, CITY]));
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-custom.employee_id"));
    api.fetchClassificationKinds.mockResolvedValue(read([SSN, CITY]));
    fireEvent.click(screen.getByTestId("classification-kind-delete-custom.employee_id"));
    fireEvent.click(
      await screen.findByTestId(
        "classification-kind-delete-custom.employee_id-confirm-dialog-confirm",
      ),
    );
    await waitFor(() =>
      expect(api.deleteClassificationKind).toHaveBeenCalledWith("custom.employee_id"),
    );
    await waitFor(() =>
      expect(screen.queryByTestId("classification-kind-custom.employee_id")).toBeNull(),
    );
  });

  it("opens the kind its deep link names", async () => {
    render(<ClassificationKindsTab title="Classification" />, {
      initialEntries: ["/context/classification?kind=custom.employee_id"],
    });
    const panel = await screen.findByTestId("classification-kind-detail-custom.employee_id");
    expect(panel).toHaveTextContent("employee id");
  });

  // REQ-2277 rule 6: the kind names its term by name, so it links to the Glossary narrowed by it.
  it("links its term to the Glossary narrowed by the term's name", async () => {
    auth.capabilities = ["sensitive_data", "glossary_read"];
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-us_ssn"));
    const link = screen.getByTestId("classification-kind-read-term-us_ssn");
    expect(link).toHaveTextContent("us social security number");
    expect(link).toHaveAttribute(
      "href",
      "/context/glossary?search=us%20social%20security%20number",
    );
  });

  it("offers no change to a caller without sensitive_data", async () => {
    auth.capabilities = ["glossary_read"];
    render(<ClassificationKindsTab title="Classification" />);
    fireEvent.click(await screen.findByTestId("classification-kind-custom.employee_id"));
    expect(screen.queryByTestId("classification-kinds-create")).toBeNull();
    expect(screen.queryByTestId("classification-kind-edit-custom.employee_id")).toBeNull();
    expect(screen.queryByTestId("classification-kind-delete-custom.employee_id")).toBeNull();
  });
});
