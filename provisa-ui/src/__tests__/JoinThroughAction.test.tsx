// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7a9f31-6e4c-4b08-95a3-8c1e7d2b6f40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1586: two ticked relationships sharing a middle table offer "Join through <table>"; its form
// is filled in with the name, many-to-many and the two ends, and saving creates the relationship.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { JoinThroughAction } from "../components/relationships/JoinThroughAction";
import type { JoinThroughPlan } from "../hooks/useRelationshipQueries";

const t = i18n.getFixedT("en");

const JUNCTION: JoinThroughPlan = {
  shape: "junction",
  name: "products-tags-via-product_tags",
  sourceTable: "products",
  sourceColumn: "id",
  targetTable: "tags",
  targetColumn: "id",
  viaTable: "product_tags",
  viaSourceColumn: "product_id",
  viaTargetColumn: "tag_id",
  attributes: ["weight", "tagged_at"],
};

let mockPlan: JoinThroughPlan | null = null;
const planFor = vi.fn();
const joinSpy = vi.fn(async () => ({ success: true, message: "saved" }));

// Spread the real module: the test registry is shared across files (see ScheduledTasks.test).
vi.mock("../hooks/useRelationshipQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useRelationshipQueries")>()),
  useJoinThroughPlan: (ids: readonly string[]) => {
    planFor(ids);
    return { plan: ids.length === 2 ? mockPlan : null, loading: false };
  },
  useJoinThrough: () => ({ joinThrough: joinSpy, saving: false }),
}));

const field = (testId: string) => screen.getByTestId(testId) as HTMLInputElement;

describe("JoinThroughAction", () => {
  beforeEach(() => {
    mockPlan = JUNCTION;
    planFor.mockClear();
    joinSpy.mockClear();
    joinSpy.mockImplementation(async () => ({ success: true, message: "saved" }));
  });

  it("is offered for two relationships sharing a middle table, named for it", () => {
    render(<JoinThroughAction ids={["pt-product", "pt-tag"]} onJoined={() => {}} />);
    expect(planFor).toHaveBeenCalledWith(["pt-product", "pt-tag"]);
    expect(screen.getByTestId("relationships-join-through")).toHaveTextContent(
      t("joinThrough.action", { table: "product_tags" }),
    );
  });

  it("is not offered for two that share no middle table", () => {
    mockPlan = null;
    render(<JoinThroughAction ids={["pt-product", "store-region"]} onJoined={() => {}} />);
    expect(screen.queryByTestId("relationships-join-through")).toBeNull();
  });

  it("is not offered for one or three ticked", () => {
    const { rerender } = render(<JoinThroughAction ids={["pt-product"]} onJoined={() => {}} />);
    expect(screen.queryByTestId("relationships-join-through")).toBeNull();
    rerender(<JoinThroughAction ids={["a", "b", "c"]} onJoined={() => {}} />);
    expect(screen.queryByTestId("relationships-join-through")).toBeNull();
  });

  it("opens a form filled in with the name, many-to-many, the two ends and the attributes", async () => {
    render(<JoinThroughAction ids={["pt-product", "pt-tag"]} onJoined={() => {}} />);
    fireEvent.click(screen.getByTestId("relationships-join-through"));
    await screen.findByTestId("join-through-modal");
    expect(field("join-through-name").value).toBe("products-tags-via-product_tags");
    expect(field("join-through-cardinality").value).toBe(t("joinThrough.manyToMany"));
    expect(field("join-through-from").value).toBe("products.id");
    expect(field("join-through-to").value).toBe("tags.id");
    expect(field("join-through-via").value).toBe("product_tags (product_id, tag_id)");
    expect(field("join-through-attributes").value).toBe("weight, tagged_at");
    expect(screen.getByTestId("join-through-shape")).toHaveTextContent(
      t("joinThrough.junction", { table: "product_tags" }),
    );
  });

  it("says a shared dimension is referred to by both, with no attributes field", async () => {
    mockPlan = {
      ...JUNCTION,
      shape: "shared_dimension",
      name: "stores-customers-via-regions",
      sourceTable: "stores",
      sourceColumn: "region_code",
      targetTable: "customers",
      targetColumn: "home_region",
      viaTable: "regions",
      viaSourceColumn: "code",
      viaTargetColumn: "code",
      attributes: [],
    };
    render(<JoinThroughAction ids={["store-region", "customer-region"]} onJoined={() => {}} />);
    fireEvent.click(screen.getByTestId("relationships-join-through"));
    await screen.findByTestId("join-through-modal");
    expect(screen.getByTestId("join-through-shape")).toHaveTextContent(
      t("joinThrough.sharedDimension", { table: "regions" }),
    );
    expect(screen.queryByTestId("join-through-attributes")).toBeNull();
  });

  it("saves under the name as edited and reports the relationship made", async () => {
    const onJoined = vi.fn();
    render(<JoinThroughAction ids={["pt-product", "pt-tag"]} onJoined={onJoined} />);
    fireEvent.click(screen.getByTestId("relationships-join-through"));
    await screen.findByTestId("join-through-modal");
    fireEvent.change(field("join-through-name"), { target: { value: "product-tagging" } });
    fireEvent.click(screen.getByTestId("join-through-save"));
    await waitFor(() => expect(onJoined).toHaveBeenCalledWith("product-tagging"));
    expect(joinSpy).toHaveBeenCalledWith("pt-product", "pt-tag", "product-tagging");
  });

  it("keeps the form open and says why when the save is refused", async () => {
    joinSpy.mockImplementation(async () => ({
      success: false,
      message: "taken",
      code: "relationship.name_taken",
      params: { kind: "id", name: "pt-tag", held_by: "pt-tag" },
    }));
    const onJoined = vi.fn();
    render(<JoinThroughAction ids={["pt-product", "pt-tag"]} onJoined={onJoined} />);
    fireEvent.click(screen.getByTestId("relationships-join-through"));
    await screen.findByTestId("join-through-modal");
    fireEvent.change(field("join-through-name"), { target: { value: "pt-tag" } });
    fireEvent.click(screen.getByTestId("join-through-save"));
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      t("serverErrors.relationship.name_taken", { name: "pt-tag", held_by: "pt-tag" }),
    );
    expect(onJoined).not.toHaveBeenCalled();
    expect(screen.getByTestId("join-through-modal")).toBeInTheDocument();
  });

  it("cannot be saved with no name", async () => {
    render(<JoinThroughAction ids={["pt-product", "pt-tag"]} onJoined={() => {}} />);
    fireEvent.click(screen.getByTestId("relationships-join-through"));
    await screen.findByTestId("join-through-modal");
    fireEvent.change(field("join-through-name"), { target: { value: "  " } });
    expect(screen.getByTestId("join-through-save")).toBeDisabled();
  });
});
