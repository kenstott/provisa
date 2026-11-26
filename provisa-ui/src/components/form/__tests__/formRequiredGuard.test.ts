// Copyright (c) 2026 Kenneth Stott
// Canary: d4101ef3-b661-442e-9dc0-512efef7caac
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968 (amended 2026-11-22): a missing or invalid field never disables a form's primary
// action, and a field that blocks submit carries the required marker. This guard reads every
// component's TSX and fails on either (requiredRules.ts).

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { checkRequiredRules, type RequiredRule } from "./requiredRules";

const SRC = resolve(__dirname, "../../..");

/**
 * Files not yet converted, each with what holds it back. Each must still fail the rules: once a
 * file passes, its entry goes, so the list only shrinks.
 */
const NOT_YET_CONVERTED: Readonly<Record<string, string>> = {
  "components/ChatPanel.tsx":
    "the primary <Button> disabled by draft; the primary <Button> disabled by otherText",
  "components/TagAssignmentForm.tsx": "<FormFooter>'s Save disabled by incomplete",
  "components/admin/AuthTab.tsx": "<FormFooter>'s Save disabled by incomplete",
  "components/admin/EncryptionTab.tsx": "<FormFooter>'s Save disabled by incomplete",
  "components/admin/FederationEngineTab.tsx": "<FormFooter>'s Save disabled by incomplete",
  "components/admin/GrantEditor.tsx":
    "<FormFooter>'s Save disabled by blank; <FormFooter>'s Save disabled by draft.url",
  "components/admin/MailPlatformsSection.tsx": "<FormFooter>'s Save disabled by platformMissing",
  "components/admin/MailTab.tsx":
    "<FormFooter>'s Save disabled by incomplete, choice; the primary <Button> disabled by testTo",
  "components/admin/MemberAccessForm.tsx": "<FormFooter>'s Save disabled by ready",
  "components/admin/OrgEncryptionTab.tsx": "the primary <Button> disabled by keyB64",
  "components/admin/OrgEngineTab.tsx": "<FormFooter>'s Save disabled by externalIncomplete",
  "components/admin/OrgFilesTab.tsx": "<FormFooter>'s Save disabled by missing",
  "components/admin/PlatformRegionsTab.tsx": "<FormFooter>'s Save disabled by id, description",
  "components/admin/RegionEngineTabs.tsx": "<FormFooter>'s Save disabled by kind, url",
  "components/admin/RelatedToDialog.tsx": "<FormFooter>'s Save disabled by name",
  "components/admin/SecretsTab.tsx": "<FormFooter>'s Save disabled by incomplete",
  "components/admin/SyntheticDatasetsPanel.tsx": "<FormFooter>'s Save disabled by name",
  "components/admin/TermEditForm.tsx": "<FormFooter>'s Save disabled by draft.name",
  "components/admin/TermReviewQueue.tsx": "the primary <Button> disabled by target",
  "components/admin/ontologyImport/ImportOntologyDialog.tsx":
    "the primary <Button> disabled by ready",
  "components/admin/ontologyImport/ItemDialogs.tsx":
    "<FormFooter>'s Save disabled by name; the primary <Button> disabled by target",
  "components/entitlements/EntitlementListForm.tsx": "<FormFooter>'s Save disabled by ready",
  "components/entitlements/ExpiryRules.tsx": "<FormFooter>'s Save disabled by reason",
  "components/lifecycle/DeprecateDialog.tsx": "the primary <Button> disabled by reason",
  "components/lifecycle/RetireDialog.tsx": "the primary <Button> disabled by reason, successor",
  "components/relationships/JoinThroughAction.tsx": "<FormFooter>'s Save disabled by name",
  "pages/MetricsPage.tsx": "<FormFooter>'s Save disabled by form.name, form.expression",
  "pages/SecurityPage.tsx": "<FormFooter>'s Save disabled by roleForm.description",
  "pages/TablesPage.tsx": "the primary <Button> disabled by regenReason",
  "pages/TeamPage.tsx": "the primary <Button> disabled by roleId",
  "pages/data-products/offers/AddOfferForm.tsx": "<FormFooter>'s Save disabled by ready",
  "pages/data-products/offers/OfferCard.tsx": "the primary <Button> disabled by ready",
  "pages/library/LibraryAddForm.tsx":
    "<TextInput value={url}> blocks submit (line 39) without required",
  "pages/security/DerivedRoles.tsx":
    "<FormFooter>'s Save disabled by attribute, description; <FormFooter>'s Save disabled by description",
  "pages/tables/CalendarCreateModal.tsx":
    "<FormFooter>'s Save disabled by name, version, retailAnchorMissing; the primary <Button> disabled by name, version, retailAnchorMissing",
};

function componentFiles(dir: string, found: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (name === "__tests__" || name === "node_modules") continue;
    if (statSync(path).isDirectory()) componentFiles(path, found);
    else if (path.endsWith(".tsx")) found.push(path);
  }
  return found;
}

const rules = (source: string): RequiredRule[] =>
  checkRequiredRules("probe.tsx", source).map((v) => v.rule);

describe("the required-field rules fail what they are meant to fail", () => {
  it("a primary action disabled by an empty field", () => {
    const form = (disabled: string) =>
      `function F() { const [name, setName] = useState("");
        return <FormLayout><FormField size="medium"><TextInput required value={name} /></FormField>
          <FormFooter onSave={save} saveProps={{ disabled: ${disabled} }} /></FormLayout>; }`;
    expect(rules(form("!name.trim()"))).toEqual(["primary-disabled-by-field"]);
    expect(rules(form(`name.trim() === ""`))).toEqual(["primary-disabled-by-field"]);
    expect(rules(form("!name"))).toEqual(["primary-disabled-by-field"]);
    expect(rules(form("incomplete"))).toEqual(["primary-disabled-by-field"]);
    expect(rules(form("missing(draft).length > 0"))).toEqual(["primary-disabled-by-field"]);
    expect(rules(form("!ready"))).toEqual(["primary-disabled-by-field"]);
  });

  it("a footer action or a submit button disabled by a missing field", () => {
    expect(
      rules(
        `<FormLayout><FormField size="medium"><select className="form-select" value={domainId} /></FormField>
          <FormFooter action={<Button variant="light" disabled={!domainId}>Go</Button>} /></FormLayout>`,
      ),
    ).toEqual(["primary-disabled-by-field"]);
    expect(rules(`<Button type="submit" disabled={domainMissing}>Go</Button>`)).toEqual([
      "primary-disabled-by-field",
    ]);
    expect(
      rules(`<FormLayout><FormField size="medium"><TextInput required value={form.name} /></FormField>
        <SaveButton disabled={!form.name.trim()} /></FormLayout>`),
    ).toEqual(["primary-disabled-by-field"]);
  });

  // REQ-2277 rule 2 (maintainer 2026-11-26): a one-field prompt is a form; its button is not
  // disabled until the value is typed, whether or not it is a form's primary action.
  it("any button disabled by an empty typed value", () => {
    const empty = (tag: string, disabled: string) =>
      rules(
        `function F() { const [reason, setReason] = useState("");
          return <Stack><TextInput value={reason} /><${tag} size="compact-xs" variant="default" disabled={${disabled}}>Go</${tag}></Stack>; }`,
      );
    expect(empty("Button", `reason.trim() === ""`)).toEqual(["action-disabled-by-empty-value"]);
    expect(empty("Button", "!reason.trim() || busy")).toEqual(["action-disabled-by-empty-value"]);
    expect(empty("Button", "!reason")).toEqual(["action-disabled-by-empty-value"]);
    expect(empty("DestructiveConfirmButton", `reason.trim() === ""`)).toEqual([
      "action-disabled-by-empty-value",
    ]);
    // A value not bound to a control on screen (an editor's text) is caught by its trim test.
    expect(empty("Button", "!sqlText.trim()")).toEqual(["action-disabled-by-empty-value"]);
    // Busy, or nothing to act on, is not an empty value.
    expect(empty("Button", "busy || rows.length === 0")).toEqual([]);
  });

  it("allows a primary action disabled when there is nothing to act on, or while it works", () => {
    expect(
      rules(
        `<FormLayout><FormField size="long"><MultiSelect value={ticked} /></FormField>
          <FormFooter action={<Button disabled={ticked.length === 0 || busy}>Register</Button>} /></FormLayout>`,
      ),
    ).toEqual([]);
    expect(
      rules(`<FormLayout><FormFooter onSave={s} saveProps={{ disabled: !dirty }} /></FormLayout>`),
    ).toEqual([]);
  });

  it("a field a submit handler refuses while empty, without the marker", () => {
    const form = (marked: string) =>
      `function F() { const submit = () => { if (!name.trim()) { setError("Name it"); return; } };
        return <FormLayout><FormField size="medium"><TextInput ${marked} value={name} /></FormField></FormLayout>; }`;
    expect(rules(form(""))).toEqual(["required-unmarked"]);
    expect(rules(form("required"))).toEqual([]);
    expect(rules(form(`{...req.field("name")}`))).toEqual([]);
  });

  it("an effect that waits for its inputs does not make them required", () => {
    expect(
      rules(
        `function F() { useEffect(() => { if (!name) return; suggest(name); }, [name]);
          return <FormLayout><FormField size="medium"><TextInput value={name} /></FormField></FormLayout>; }`,
      ),
    ).toEqual([]);
  });

  it("a required field never spread onto its control, and a native select without its marker", () => {
    expect(
      rules(
        `function F() { const req = useRequiredFields({ name, ...(asked ? { domain } : {}) });
          return <FormLayout><FormField size="medium"><TextInput {...req.field("name")} value={name} /></FormField></FormLayout>; }`,
      ),
    ).toEqual(["required-unmarked"]);
    // Handed to a component, the fields are spread in that component's own file.
    expect(
      rules(
        `function F() { const req = useRequiredFields({ name, domain });
          return <FormLayout><Identifiers required={req} /></FormLayout>; }`,
      ),
    ).toEqual([]);
    expect(
      rules(
        `function F() { const req = useRequiredFields({ domain });
          return <FormLayout><FormField size="medium"><label>Domain
            <select className="form-select" {...req.native("domain")} value={domain} /></label></FormField></FormLayout>; }`,
      ),
    ).toEqual(["required-unmarked"]);
    expect(
      rules(
        `function F() { const req = useRequiredFields({ domain });
          return <FormLayout><FormField size="medium"><label>Domain<RequiredMark />
            <select className="form-select" {...req.native("domain")} value={domain} /></label></FormField></FormLayout>; }`,
      ),
    ).toEqual([]);
  });
});

describe("every component follows the required-field rules", () => {
  const files = componentFiles(SRC).map((path) => ({ path, rel: relative(SRC, path) }));

  it("finds the components", () => {
    expect(files.length).toBeGreaterThan(100);
  });

  it.each(Object.keys(NOT_YET_CONVERTED))("%s still exists", (rel) => {
    expect(existsSync(join(SRC, rel))).toBe(true);
  });

  it.each(Object.keys(NOT_YET_CONVERTED))("%s still needs its not-yet-converted entry", (rel) => {
    expect(checkRequiredRules(rel, readFileSync(join(SRC, rel), "utf8"))).not.toEqual([]);
  });

  it.each(files.filter((f) => !(f.rel in NOT_YET_CONVERTED)).map((f) => [f.rel, f.path]))(
    "%s",
    (rel, path) => {
      const violations = checkRequiredRules(rel, readFileSync(path, "utf8"));
      expect(violations.map((v) => `${rel}:${v.line} ${v.rule}: ${v.detail}`).join("\n")).toBe("");
    },
  );
});
