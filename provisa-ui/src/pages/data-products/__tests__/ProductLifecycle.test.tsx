// Copyright (c) 2026 Kenneth Stott
// Canary: bca6b0d4-d583-45f8-9ce1-1b71cecb10e7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634 STATUS, REQ-2146: a data product's lifecycle on its panel. Each status offers only the
// actions the server takes from it; a retirement's successor is a product of the same domain in
// service, required while the product has members; a refusal is shown in the reader's language;
// each finding is a chip whose fixes go where it is fixed.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import { useLocation } from "react-router-dom";
import { useProductLifecycle } from "../ProductLifecycle";
import { DetailFields } from "../../../components/detail/DetailFields";
import type { DataProduct, MutationResult } from "../../../types/admin";
import type { DataProductStatus, ProductFinding } from "../../../hooks/useDataProductMutations";

const product = (id: string, over: Partial<DataProduct> = {}): DataProduct => ({
  id,
  domainId: "sales",
  name: id.toUpperCase(),
  purpose: "",
  limitations: "",
  usage: "",
  status: "live",
  customProperties: {},
  ...over,
});

const answer = (r: MutationResult) => vi.fn(() => Promise.resolve(r));
const done = (code: string, id = "c360") =>
  answer({ success: true, message: "", code: `data_product.status.${code}`, params: { data_product: id } });

function actions(over: Partial<DataProductStatus> = {}): DataProductStatus {
  return {
    publish: done("published"),
    deprecate: done("deprecated"),
    retire: done("retired"),
    restore: done("restored"),
    ...over,
  };
}

function Where() {
  const { pathname, search } = useLocation();
  return <span data-testid="where">{pathname + search}</span>;
}

function Lifecycle(props: {
  product: DataProduct;
  products?: DataProduct[];
  members?: string[];
  findings?: ProductFinding[];
  canEdit?: boolean;
  status: DataProductStatus;
  onChanged?: (m: string) => void;
  onFailed?: (m: string) => void;
  onEdit?: () => void;
  openProduct?: (id: string) => void;
}) {
  const view = useProductLifecycle({
    product: props.product,
    products: props.products ?? [props.product],
    members: props.members ?? [],
    findings: props.findings ?? [],
    canEdit: props.canEdit ?? true,
    status: props.status,
    onChanged: props.onChanged ?? vi.fn(),
    onFailed: props.onFailed ?? vi.fn(),
    onEdit: props.onEdit ?? vi.fn(),
    openProduct: props.openProduct ?? vi.fn(),
  });
  return (
    <>
      <DetailFields testId="fields" fields={view.fields} />
      <div data-testid="actions">{view.actions}</div>
      {view.toReview}
      {view.dialogs}
      <Where />
    </>
  );
}

const shown = () =>
  within(screen.getByTestId("actions"))
    .queryAllByRole("button")
    .map((b) => b.getAttribute("data-testid"));

describe("a data product's status actions", () => {
  it.each([
    ["draft", ["data-product-publish-c360"]],
    ["live", ["data-product-deprecate-c360", "data-product-retire-c360"]],
    ["deprecated", ["data-product-retire-c360", "data-product-restore-c360"]],
    ["retired", ["data-product-restore-c360"]],
  ] as const)("are, for a %s product, the ones the server takes from it", (status, ids) => {
    render(<Lifecycle product={product("c360", { status })} status={actions()} />);
    expect(shown()).toEqual(ids);
  });

  it("are none for a reader who cannot change the product", () => {
    render(<Lifecycle product={product("c360")} canEdit={false} status={actions()} />);
    expect(shown()).toEqual([]);
  });

  it("publish a draft, and tell the reader in their language", async () => {
    const status = actions();
    const onChanged = vi.fn();
    render(
      <Lifecycle product={product("c360", { status: "draft" })} status={status} onChanged={onChanged} />,
    );
    fireEvent.click(screen.getByTestId("data-product-publish-c360"));
    await waitFor(() => expect(onChanged).toHaveBeenCalledWith("Data product c360 published."));
    expect(status.publish).toHaveBeenCalledWith("c360");
  });

  it("show a refusal by its code", async () => {
    const onFailed = vi.fn();
    render(
      <Lifecycle
        product={product("c360", { status: "retired" })}
        status={actions({
          restore: answer({
            success: false,
            message: "server words",
            code: "data_product.status.not_restorable",
            params: { data_product: "c360", status: "live" },
          }),
        })}
        onFailed={onFailed}
      />,
    );
    fireEvent.click(screen.getByTestId("data-product-restore-c360"));
    await waitFor(() =>
      expect(onFailed).toHaveBeenCalledWith(
        "Only a deprecated or retired data product can be restored; this one is live.",
      ),
    );
  });

  it("deprecate with a reason, the planned retirement left open", async () => {
    const status = actions();
    const onChanged = vi.fn();
    render(<Lifecycle product={product("c360")} status={status} onChanged={onChanged} />);
    fireEvent.click(screen.getByTestId("data-product-deprecate-c360"));
    const submit = await screen.findByTestId("data-product-deprecate-submit");
    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByTestId("data-product-deprecate-reason"), {
      target: { value: "Replaced by c360 v2" },
    });
    fireEvent.click(submit);
    await waitFor(() => expect(onChanged).toHaveBeenCalledWith("Data product c360 deprecated."));
    expect(status.deprecate).toHaveBeenCalledWith("c360", "Replaced by c360 v2", null);
  });
});

describe("a data product's retirement", () => {
  const products = [
    product("c360"),
    product("c360_v2", { name: "Customer 360 v2" }),
    product("old", { status: "retired" }),
    product("ledger", { domainId: "finance" }),
  ];

  it("chooses its successor among the products of its domain in service, and needs one while it has members", async () => {
    const status = actions({ retire: done("retired") });
    const onChanged = vi.fn();
    render(
      <Lifecycle
        product={products[0]}
        products={products}
        members={["customers", "refresh_customers"]}
        status={status}
        onChanged={onChanged}
      />,
    );
    fireEvent.click(screen.getByTestId("data-product-retire-c360"));
    expect(await screen.findByTestId("data-product-retire-moves")).toHaveTextContent(
      "Members that move: customers, refresh_customers",
    );
    fireEvent.change(screen.getByTestId("data-product-retire-reason"), {
      target: { value: "Merged" },
    });
    const submit = screen.getByTestId("data-product-retire-submit");
    // A reason alone is not enough: its members need a successor to move to.
    expect(submit).toBeDisabled();
    fireEvent.click(screen.getByTestId("data-product-retire-successor"));
    const options = await screen.findAllByRole("option", { hidden: true });
    expect(options.map((o) => o.textContent)).toEqual(["Customer 360 v2 (c360_v2)"]);
    fireEvent.click(options[0]);
    expect(submit).toBeEnabled();
    fireEvent.click(submit);
    await waitFor(() => expect(onChanged).toHaveBeenCalledWith("Data product c360 retired."));
    expect(status.retire).toHaveBeenCalledWith("c360", "Merged", "c360_v2");
  });

  it("needs no successor when it has no members, and shows a refusal in the dialog", async () => {
    const status = actions({
      retire: answer({
        success: false,
        message: "",
        code: "data_product.status.reason_required",
        params: { data_product: "c360" },
      }),
    });
    render(<Lifecycle product={products[0]} products={products} status={status} />);
    fireEvent.click(screen.getByTestId("data-product-retire-c360"));
    expect(await screen.findByTestId("data-product-retire-moves")).toHaveTextContent(
      "It has no members: a successor is optional.",
    );
    fireEvent.change(screen.getByTestId("data-product-retire-reason"), {
      target: { value: "Gone" },
    });
    fireEvent.click(screen.getByTestId("data-product-retire-submit"));
    expect(await screen.findByTestId("data-product-retire-error")).toHaveTextContent(
      "A reason is required: say why the data product is deprecated or retired.",
    );
    expect(status.retire).toHaveBeenCalledWith("c360", "Gone", null);
  });
});

describe("a data product's status fields", () => {
  it("show a deprecated product's reason and planned retirement", () => {
    render(
      <Lifecycle
        product={product("c360", {
          status: "deprecated",
          statusReason: "Replaced",
          retireOn: "2026-12-01",
        })}
        status={actions()}
      />,
    );
    const rows = within(screen.getByTestId("fields"))
      .getAllByRole("term")
      .map((dt) => [dt.textContent, dt.nextElementSibling?.textContent]);
    expect(rows).toEqual([
      ["Status", "Deprecated"],
      ["Status reason", "Replaced"],
      ["Planned retirement", "2026-12-01"],
    ]);
  });

  it("name a retired product's successor, which opens it", () => {
    const openProduct = vi.fn();
    render(
      <Lifecycle
        product={product("c360", { status: "retired", statusReason: "Merged", successorId: "v2" })}
        products={[product("c360"), product("v2", { name: "Customer 360 v2" })]}
        status={actions()}
        openProduct={openProduct}
      />,
    );
    const successor = screen.getByTestId("data-product-successor-c360");
    expect(successor).toHaveTextContent("Customer 360 v2");
    fireEvent.click(successor);
    expect(openProduct).toHaveBeenCalledWith("v2");
  });
});

const finding = (kind: string, params: Record<string, string>): ProductFinding => ({
  kind,
  code: `data_product.finding.${kind}`,
  message: "",
  params,
});

describe("a data product's findings", () => {
  const findings = [
    finding("member_deprecated", { table_id: "42", table: "customers", expires_on: "" }),
    finding("successor_retired", { successor: "v2" }),
    finding("retirement_blocked", { retire_on: "2026-10-01" }),
    finding("no_owner", {}),
    finding("no_support_contact", {}),
  ];
  const chip = (i: number) => screen.getByTestId(`data-product-c360-finding-${i}`);
  const fixes = () =>
    within(screen.getByTestId("data-product-c360-finding-detail"))
      .queryAllByRole("button")
      .map((b) => b.textContent);

  it("are chips named by their kind, each message in the reader's language", () => {
    render(<Lifecycle product={product("c360")} findings={findings} status={actions()} />);
    expect(findings.map((_, i) => chip(i).textContent)).toEqual([
      "Member deprecated",
      "Successor retired",
      "Retirement blocked",
      "No owner",
      "No support contact",
    ]);
    fireEvent.click(chip(0));
    expect(screen.getByTestId("data-product-c360-finding-detail")).toHaveTextContent(
      "Member table customers is deprecated.",
    );
  });

  it("fix a deprecated member on its table's page", () => {
    render(<Lifecycle product={product("c360")} findings={findings} status={actions()} />);
    fireEvent.click(chip(0));
    fireEvent.click(screen.getByText("Open customers"));
    expect(screen.getByTestId("where")).toHaveTextContent("/tables?table=42");
  });

  it("open a retired successor", () => {
    const openProduct = vi.fn();
    render(
      <Lifecycle
        product={product("c360")}
        findings={findings}
        status={actions()}
        openProduct={openProduct}
      />,
    );
    fireEvent.click(chip(1));
    fireEvent.click(screen.getByText("Open v2"));
    expect(openProduct).toHaveBeenCalledWith("v2");
  });

  it("choose a successor for a blocked retirement, and edit for the rest; a reader who cannot edit gets neither", async () => {
    const onEdit = vi.fn();
    const { unmount } = render(
      <Lifecycle product={product("c360")} findings={findings} status={actions()} onEdit={onEdit} />,
    );
    fireEvent.click(chip(2));
    fireEvent.click(screen.getByText("Choose a successor…"));
    expect(await screen.findByTestId("data-product-retire-reason")).toBeInTheDocument();
    fireEvent.click(chip(3));
    fireEvent.click(screen.getByText("Edit product"));
    expect(onEdit).toHaveBeenCalled();
    // The form opens on the support contact.
    fireEvent.click(chip(4));
    fireEvent.click(screen.getByText("Edit product"));
    expect(onEdit).toHaveBeenLastCalledWith("supportContact");
    unmount();

    render(
      <Lifecycle product={product("c360")} findings={findings} status={actions()} canEdit={false} />,
    );
    fireEvent.click(chip(2));
    expect(fixes()).toEqual([]);
    fireEvent.click(chip(3));
    expect(fixes()).toEqual([]);
    fireEvent.click(chip(4));
    expect(fixes()).toEqual([]);
  });
});
