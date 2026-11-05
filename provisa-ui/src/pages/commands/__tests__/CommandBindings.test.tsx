// Copyright (c) 2026 Kenneth Stott
// Canary: 4b9e1d72-8c35-4a60-b2f7-3e8d6c1a5f94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A command's Bindings tab: its name in each language, its own name where none is declared, a
// language it is left out of said so; the form renames it or leaves it out, never out of SQL.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { CommandBindingsFields, CommandBindingsRead } from "../CommandBindings";

describe("CommandBindingsRead", () => {
  it("shows each language's name, its own where none is declared, and where it is left out", () => {
    render(
      <CommandBindingsRead
        name="meta_schema"
        bindings={{ cypher: { name: "apoc.meta.schema" }, graphql: { exclude: true } }}
      />,
    );
    expect(screen.getByTestId("command-binding-read-cypher")).toHaveTextContent(
      "apoc.meta.schema",
    );
    expect(screen.getByTestId("command-binding-read-sql")).toHaveTextContent("meta_schema");
    expect(screen.getByTestId("command-binding-read-graphql")).toHaveTextContent("Not offered");
  });
});

describe("CommandBindingsFields", () => {
  it("renames the command in a language", () => {
    const onChange = vi.fn();
    render(<CommandBindingsFields name="meta_schema" bindings={{}} onChange={onChange} />);
    fireEvent.change(screen.getByTestId("command-binding-name-cypher"), {
      target: { value: "apoc.meta.schema" },
    });
    expect(onChange).toHaveBeenCalledWith({ cypher: { name: "apoc.meta.schema" } });
  });

  it("leaves the command out of a language, and back in", () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <CommandBindingsFields name="meta_schema" bindings={{}} onChange={onChange} />,
    );
    fireEvent.click(screen.getByTestId("command-binding-exclude-graphql"));
    expect(onChange).toHaveBeenLastCalledWith({ graphql: { exclude: true } });
    rerender(
      <CommandBindingsFields
        name="meta_schema"
        bindings={{ graphql: { exclude: true } }}
        onChange={onChange}
      />,
    );
    expect(screen.getByTestId("command-binding-name-graphql")).toBeDisabled();
    fireEvent.click(screen.getByTestId("command-binding-exclude-graphql"));
    expect(onChange).toHaveBeenLastCalledWith({});
  });

  it("offers no way to leave the command out of SQL", () => {
    render(<CommandBindingsFields name="meta_schema" bindings={{}} onChange={vi.fn()} />);
    expect(screen.queryByTestId("command-binding-exclude-sql")).toBeNull();
    expect(screen.getByText("Always offered")).toBeInTheDocument();
  });
});
