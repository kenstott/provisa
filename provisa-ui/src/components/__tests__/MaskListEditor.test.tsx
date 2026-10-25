// Copyright (c) 2026 Kenneth Stott
// Canary: 33f8632e-12f2-4328-b8b7-3a1efb7ce95b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A column's ordered mask list (REQ-2059): a new list starts with one entry for everyone, an entry
// is added before it, entries move and go, and the server's check is shown -- entries no reader
// reaches, a list that leaves readers it does not name unmasked, the entry chosen roles read
// through.

import { useState } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";
import { MaskListEditor } from "../MaskListEditor";
import {
  filledTemplate,
  maskValueOf,
  moved,
  newMaskList,
  withNewEntry,
  type MaskEntry,
} from "../../lib/masks";
import en from "../../i18n/locales/en/maskList.json";

const checkMasks = vi.fn();
const maskPresets = vi.fn();
vi.mock("../../api/admin", () => ({
  checkMasks: (...args: unknown[]) => checkMasks(...args),
  maskPresets: () => maskPresets(),
}));

// The presets the server offers, as GET /admin/masks/presets answers.
const preset = (id: string, pattern: string, replace: string, input: string, output: string) => ({
  id,
  label: id,
  pattern,
  replace,
  example: { input, output },
});
const PRESETS = [
  preset(
    "ssn_last4",
    "^[0-9]{3}-?[0-9]{2}-?([0-9]{4})$",
    "***-**-$1",
    "123-45-6789",
    "***-**-6789",
  ),
  preset(
    "card_last4",
    "^[0-9 -]*([0-9]{4})$",
    "**** **** **** $1",
    "4111 1111 1111 1234",
    "**** **** **** 1234",
  ),
  preset("phone_last4", "^.*([0-9]{4})$", "***-***-$1", "(415) 555-0123", "***-***-0123"),
  preset(
    "email_initial_domain",
    "^(.)[^@]*(@.*)$",
    "$1***$2",
    "jane@example.com",
    "j***@example.com",
  ),
  preset("all_but_last4", "^.*(.{4})$", "****$1", "ABCDEFGH", "****EFGH"),
  preset("digits_keep_separators", "[0-9]", "*", "415-555-0123", "***-***-****"),
  preset("zip3", "^([0-9]{3})[0-9]{2}(-[0-9]{4})?$", "$1**", "94110", "941**"),
  preset("birth_year", "^([0-9]{4})-[0-9]{2}-[0-9]{2}", "$1-**-**", "1984-07-21", "1984-**-**"),
  preset(
    "iban_last4",
    "^([A-Z]{2})[0-9A-Z]*([0-9A-Z]{4})$",
    "$1** **** $2",
    "DE89370400440532013000",
    "DE** **** 3000",
  ),
  {
    id: "keep_first_last",
    label: "keep_first_last",
    params: ["first", "last"],
    pattern_template: "^(.{{first}}).*(.{{last}})$",
    replace: "$1***$2",
    example: { input: "Johnathan", output: "Jo***an" },
  },
];

const ROLES = [
  { id: "analyst", label: "analyst" },
  { id: "contractor", label: "contractor" },
];

function Editor({ initial, seen }: { initial: MaskEntry[]; seen?: (m: MaskEntry[]) => void }) {
  const [value, setValue] = useState(initial);
  return (
    <MaskListEditor
      value={value}
      onChange={(next) => {
        seen?.(next);
        setValue(next);
      }}
      roleOptions={ROLES}
    />
  );
}

const report = (over: object = {}) => ({
  unreachable: [],
  unlisted_unmasked: false,
  applies: null,
  patterns: [],
  ...over,
});

beforeEach(() => {
  checkMasks.mockReset();
  checkMasks.mockResolvedValue(report());
  maskPresets.mockReset();
  maskPresets.mockResolvedValue(PRESETS);
});

describe("the list's helpers", () => {
  it("starts a list with one entry for everyone, masked to NULL", () => {
    expect(newMaskList()).toEqual([{ to: ["*"], mask: { type: "constant", value: null } }]);
  });

  it("adds an entry before the final entry for everyone", () => {
    const list = withNewEntry(newMaskList());
    expect(list).toEqual([{ to: [], mask: null }, newMaskList()[0]]);
    expect(withNewEntry([{ to: ["analyst"], mask: null }])).toEqual([
      { to: ["analyst"], mask: null },
      { to: [], mask: null },
    ]);
  });

  it("moves an entry within the list only", () => {
    const list: MaskEntry[] = [
      { to: ["a"], mask: null },
      { to: ["b"], mask: null },
    ];
    expect(moved(list, 0, 1).map((e) => e.to[0])).toEqual(["b", "a"]);
    expect(moved(list, 0, -1)).toBe(list);
  });

  it("keeps a constant's number a number and blank a NULL", () => {
    expect(maskValueOf("0")).toBe(0);
    expect(maskValueOf("2.5")).toBe(2.5);
    expect(maskValueOf("***")).toBe("***");
    expect(maskValueOf("  ")).toBeNull();
  });
});

describe("MaskListEditor", () => {
  it("starts an empty list with its entry for everyone", () => {
    const seen = vi.fn();
    render(<Editor initial={[]} seen={seen} />);
    fireEvent.click(screen.getByTestId("mask-list-start"));
    expect(seen).toHaveBeenLastCalledWith(newMaskList());
    expect(screen.getByTestId("mask-list-entry-0")).toBeInTheDocument();
    expect(screen.queryByTestId("mask-list-entry-1")).toBeNull();
  });

  it("adds an entry before everyone's and removes one", () => {
    const seen = vi.fn();
    render(<Editor initial={newMaskList()} seen={seen} />);
    fireEvent.click(screen.getByTestId("mask-list-add"));
    expect(seen).toHaveBeenLastCalledWith([{ to: [], mask: null }, newMaskList()[0]]);
    fireEvent.click(screen.getByRole("button", { name: "Remove entry 1" }));
    expect(seen).toHaveBeenLastCalledWith(newMaskList());
  });

  it("flags an entry an earlier one covers, naming it", async () => {
    checkMasks.mockResolvedValue(report({ unreachable: [{ entry: 1, covered_by: 0 }] }));
    render(
      <Editor
        initial={[
          { to: ["analyst"], mask: null },
          { to: ["analyst"], mask: { type: "constant", value: null } },
        ]}
      />,
    );
    expect(await screen.findByTestId("mask-list-unreachable-1")).toHaveTextContent(
      "No reader reaches this entry: entry 1 applies to each of them first.",
    );
    expect(screen.queryByTestId("mask-list-unreachable-0")).toBeNull();
  });

  it("warns when readers it does not name read the column unmasked", async () => {
    checkMasks.mockResolvedValue(report({ unlisted_unmasked: true }));
    render(<Editor initial={[{ to: ["contractor"], mask: { type: "constant", value: null } }]} />);
    expect(await screen.findByTestId("mask-list-unlisted")).toHaveTextContent(
      "Readers this list does not name see the column unmasked.",
    );
  });

  it("asks which entry the chosen roles read through and marks it", async () => {
    render(<Editor initial={newMaskList()} />);
    await waitFor(() => expect(checkMasks).toHaveBeenCalledWith(newMaskList(), undefined));
    checkMasks.mockResolvedValue(report({ applies: 0 }));
    const input = screen.getByRole("textbox", { name: "Check access" });
    fireEvent.change(input, { target: { value: "analyst" } });
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() => expect(checkMasks).toHaveBeenLastCalledWith(newMaskList(), ["analyst"]));
    expect(await screen.findByTestId("mask-list-applies")).toHaveTextContent("Entry 1 applies");
    expect(screen.getByTestId("mask-list-entry-0")).toHaveAttribute("data-applies", "true");
  });

  it("sets a constant's value as typed, and an unmasked entry has no value", () => {
    const seen = vi.fn();
    render(<Editor initial={[{ to: ["analyst"], mask: null }, ...newMaskList()]} seen={seen} />);
    // One value field: the unmasked entry has none.
    const [value] = screen.getAllByLabelText("Constant value");
    expect(screen.getAllByLabelText("Constant value")).toHaveLength(1);
    fireEvent.change(value, { target: { value: "0" } });
    expect(seen).toHaveBeenLastCalledWith([
      { to: ["analyst"], mask: null },
      { to: ["*"], mask: { type: "constant", value: 0 } },
    ]);
  });
});

describe("a regex entry's common masks and the universal dialect", () => {
  const REGEX: MaskEntry[] = [{ to: ["*"], mask: { type: "regex", pattern: "", replace: "" } }];
  const field = (name: "pattern" | "replace") =>
    screen.getByTestId(`mask-list-regex-0-${name}`).querySelector("input") ??
    (screen.getByTestId(`mask-list-regex-0-${name}`) as HTMLInputElement);
  const value = (name: "pattern" | "replace") => (field(name) as HTMLInputElement).value;

  async function pick(label: string) {
    await waitFor(() => expect(screen.getByTestId("mask-list-regex-0-presets")).toBeEnabled());
    fireEvent.click(screen.getByTestId("mask-list-regex-0-presets"));
    fireEvent.click(within(await screen.findByRole("menu")).getByText(label));
  }

  for (const p of PRESETS.filter((x): x is ReturnType<typeof preset> => !("params" in x)))
    it(`fills both fields from ${p.id}, labelled in the catalog`, async () => {
      render(<Editor initial={REGEX} />);
      await pick(en.maskList.preset[p.id as keyof typeof en.maskList.preset]);
      await waitFor(() => expect(value("pattern")).toBe(p.pattern));
      expect(value("replace")).toBe(p.replace);
    });

  it("fills keep-first-and-last with the numbers chosen, and both fields stay editable", async () => {
    render(<Editor initial={REGEX} />);
    await waitFor(() => expect(screen.getByTestId("mask-list-regex-0-presets")).toBeEnabled());
    fireEvent.click(screen.getByTestId("mask-list-regex-0-presets"));
    const menu = await screen.findByRole("menu");
    const first =
      within(screen.getByTestId("mask-list-regex-0-preset-keep_first_last-first")).queryByRole(
        "textbox",
      ) ?? screen.getByLabelText("First");
    fireEvent.change(first, { target: { value: "3" } });
    fireEvent.click(within(menu).getByText(en.maskList.preset.keep_first_last));
    await waitFor(() => expect(value("pattern")).toBe("^(.{3}).*(.{2})$"));
    expect(value("replace")).toBe("$1***$2");
    fireEvent.change(field("replace"), { target: { value: "$1-$2" } });
    expect(value("replace")).toBe("$1-$2");
  });

  it("names a construct outside the universal dialect under its field, in red", async () => {
    checkMasks.mockResolvedValue(
      report({
        patterns: [
          {
            entry: 0,
            field: "pattern",
            ok: false,
            construct: "lookahead",
            position: 3,
            message: "x",
          },
        ],
      }),
    );
    render(
      <Editor
        initial={[{ to: ["*"], mask: { type: "regex", pattern: "^.*(?=.{4})", replace: "*" } }]}
      />,
    );
    expect(
      await screen.findByText("Not on every engine: a lookahead (?=…) at character 4"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("mask-list-regex-0-universal")).toBeNull();
  });

  it("says quietly that a pattern the server passes works on every engine", async () => {
    render(
      <Editor initial={[{ to: ["*"], mask: { type: "regex", pattern: "[0-9]", replace: "*" } }]} />,
    );
    expect(await screen.findByTestId("mask-list-regex-0-universal")).toHaveTextContent(
      "Works on every engine",
    );
  });

  it("shows a warning the server passes with, in orange, without refusing", async () => {
    checkMasks.mockResolvedValue(
      report({
        patterns: [
          {
            entry: 0,
            field: "pattern",
            ok: true,
            construct: "alternation_overlap",
            position: 0,
            message: "x",
          },
        ],
      }),
    );
    render(
      <Editor initial={[{ to: ["*"], mask: { type: "regex", pattern: "a|ab", replace: "*" } }]} />,
    );
    expect(await screen.findByTestId("mask-list-regex-0-warning")).toHaveTextContent(
      "alternatives that can match the same text",
    );
    expect(screen.getByTestId("mask-list-regex-0-universal")).toBeInTheDocument();
  });

  it("fills a template's named numbers, leaving a regex's own braces", () => {
    expect(filledTemplate("^(.{{first}}).*(.{{last}})$", { first: 2, last: 3 })).toBe(
      "^(.{2}).*(.{3})$",
    );
    expect(filledTemplate("^[0-9]{3}(.{{first}})$", { first: 1 })).toBe("^[0-9]{3}(.{1})$");
  });
});
