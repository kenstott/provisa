// Copyright (c) 2026 Kenneth Stott
// Canary: 0fd838d4-c841-42bc-84c5-96b9d4fa0aad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993: Admin › Package registries holds the npm registries, PyPI indexes and Git
// hosts every Node and Python install of the organisation goes through. Each field is shown again as it was entered, a new entry is
// added with nothing in it, and an entry is not saved until it has its address and credential.
// REQ-2088: each entry may carry the PEM certificate authority its server's TLS chains to.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";
import en from "../i18n/locales/en/packageRegistries.json";

const auth = vi.hoisted(() => ({
  capabilities: ["org_settings"] as string[],
  activeOrgId: "acme" as string | null,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../api/packageRegistries", () => ({
  fetchPackageRegistries: vi.fn(),
  addPackageRegistry: vi.fn(),
  replacePackageRegistry: vi.fn(),
  deletePackageRegistry: vi.fn(),
}));

import {
  addPackageRegistry,
  deletePackageRegistry,
  fetchPackageRegistries,
  replacePackageRegistry,
} from "../api/packageRegistries";
import { PackageRegistriesTab } from "../components/admin/PackageRegistriesTab";
import { NAV_GROUPS } from "../components/navGroups";

const list = vi.mocked(fetchPackageRegistries);
const add = vi.mocked(addPackageRegistry);
const replace = vi.mocked(replacePackageRegistry);
const remove = vi.mocked(deletePackageRegistry);

const NPM = {
  id: "r1",
  kind: "npm" as const,
  address: "https://npm.acme.test/",
  scope: "@acme",
  credential: "${secret:npm_acme}",
  mirror: false,
  ca_bundle: null,
};
const GIT = {
  id: "g1",
  kind: "git" as const,
  address: "git.acme.test",
  scope: null,
  credential: "ada:made-up-git-token",
  mirror: false,
  ca_bundle: "${secret:git_ca}",
};

beforeEach(() => {
  list.mockReset().mockResolvedValue([NPM, GIT]);
  add.mockReset().mockResolvedValue(NPM);
  replace.mockReset().mockResolvedValue(NPM);
  remove.mockReset().mockResolvedValue(undefined);
});

describe("Admin › Package registries", () => {
  it("is an organisation setting in the admin navigation", () => {
    const items = NAV_GROUPS.flatMap((g) => g.items);
    const item = items.find((i) => i.to === "/admin/package-registries");
    expect(item?.capability).toBe("org_settings");
  });

  it("shows each entry's fields as they were entered, under npm or Git", async () => {
    render(<PackageRegistriesTab />);
    expect(await screen.findByTestId("package-registry-r1")).toBeInTheDocument();
    expect(screen.getByTestId("package-registry-r1-address")).toHaveValue(NPM.address);
    expect(screen.getByTestId("package-registry-r1-scope")).toHaveValue("@acme");
    expect(screen.getByTestId("package-registry-r1-credential")).toHaveValue(NPM.credential);
    expect(screen.getByTestId("package-registries-git")).toContainElement(
      screen.getByTestId("package-registry-g1"),
    );
    // A Git host has no scope and no mirror.
    expect(screen.queryByTestId("package-registry-g1-scope")).toBeNull();
    expect(screen.queryByTestId("package-registry-g1-mirror")).toBeNull();
  });

  it("says the public npm registry is used when no npm registry is entered", async () => {
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab />);
    expect(await screen.findByText(en.packageRegistries.npmNone)).toBeInTheDocument();
  });

  it("adds an empty entry that is saved only once it has its address and credential", async () => {
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab />);
    fireEvent.click(await screen.findByTestId("package-registries-add-npm"));
    const card = await screen.findByTestId(/^package-registry-new-npm-\d+$/);
    const id = card.getAttribute("data-testid") as string;
    expect(screen.getByTestId(`${id}-save`)).toBeDisabled();
    fireEvent.change(screen.getByTestId(`${id}-address`), {
      target: { value: "https://mirror.acme.test/" },
    });
    expect(screen.getByTestId(`${id}-save`)).toBeDisabled();
    fireEvent.change(screen.getByTestId(`${id}-credential`), { target: { value: "tok" } });
    fireEvent.click(screen.getByTestId(`${id}-mirror`));
    fireEvent.click(screen.getByTestId(`${id}-save`));
    await waitFor(() =>
      expect(add).toHaveBeenCalledWith("acme", {
        kind: "npm",
        address: "https://mirror.acme.test/",
        scope: null,
        credential: "tok",
        mirror: true,
        ca_bundle: null,
      }),
    );
  });

  it("adds a PyPI index with no scope, which may be the default index", async () => {
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab />);
    expect(await screen.findByTestId("package-registries-pypi-none")).toHaveTextContent(
      en.packageRegistries.pypiNone,
    );
    fireEvent.click(screen.getByTestId("package-registries-add-pypi"));
    const card = await screen.findByTestId(/^package-registry-new-pypi-\d+$/);
    const id = card.getAttribute("data-testid") as string;
    expect(screen.queryByTestId(`${id}-scope`)).toBeNull();
    fireEvent.change(screen.getByTestId(`${id}-address`), {
      target: { value: "https://pypi.acme.test/simple/" },
    });
    fireEvent.change(screen.getByTestId(`${id}-credential`), { target: { value: "tok" } });
    fireEvent.click(screen.getByTestId(`${id}-mirror`));
    fireEvent.click(screen.getByTestId(`${id}-save`));
    await waitFor(() =>
      expect(add).toHaveBeenCalledWith("acme", {
        kind: "pypi",
        address: "https://pypi.acme.test/simple/",
        scope: null,
        credential: "tok",
        mirror: true,
        ca_bundle: null,
      }),
    );
  });

  it("replaces a kept entry by its id", async () => {
    render(<PackageRegistriesTab />);
    fireEvent.change(await screen.findByTestId("package-registry-r1-address"), {
      target: { value: "https://npm2.acme.test/" },
    });
    fireEvent.click(screen.getByTestId("package-registry-r1-save"));
    await waitFor(() =>
      expect(replace).toHaveBeenCalledWith("acme", "r1", {
        kind: "npm",
        address: "https://npm2.acme.test/",
        scope: "@acme",
        credential: NPM.credential,
        mirror: false,
        ca_bundle: null,
      }),
    );
  });

  it("removes a kept entry, and drops a new one without asking the server", async () => {
    render(<PackageRegistriesTab />);
    fireEvent.click(await screen.findByTestId("package-registry-g1-remove"));
    await waitFor(() => expect(remove).toHaveBeenCalledWith("acme", "g1"));
    fireEvent.click(screen.getByTestId("package-registries-add-git"));
    const card = await screen.findByTestId(/^package-registry-new-git-\d+$/);
    fireEvent.click(screen.getByTestId(`${card.getAttribute("data-testid")}-remove`));
    await waitFor(() => expect(screen.queryByTestId(/^package-registry-new-git-/)).toBeNull());
    expect(remove).toHaveBeenCalledTimes(1);
  });

  it("shows each entry's certificate authority as entered, and sends it on save", async () => {
    render(<PackageRegistriesTab />);
    expect(await screen.findByTestId("package-registry-g1-ca-bundle")).toHaveValue(
      "${secret:git_ca}",
    );
    expect(screen.getByTestId("package-registry-r1-ca-bundle")).toHaveValue("");
    const pem = "-----BEGIN CERTIFICATE-----\nMIIBmade-up\n-----END CERTIFICATE-----";
    fireEvent.change(screen.getByTestId("package-registry-r1-ca-bundle"), {
      target: { value: pem },
    });
    fireEvent.click(screen.getByTestId("package-registry-r1-save"));
    await waitFor(() =>
      expect(replace).toHaveBeenCalledWith(
        "acme",
        "r1",
        expect.objectContaining({ ca_bundle: pem }),
      ),
    );
  });

  it("names Artifactory and each Git provider's user in its help", async () => {
    render(<PackageRegistriesTab />);
    await screen.findByTestId("package-registry-r1");
    expect(en.packageRegistries.npmCredentialHelp).toMatch(/Artifactory/);
    expect(en.packageRegistries.caBundleHelp).toMatch(/Artifactory/);
    expect(en.packageRegistries.gitCredentialHelp).toMatch(/x-access-token.*GitLab.*x-token-auth/s);
    // Each help is the name of its field's (?) glyph.
    expect(
      screen.getAllByRole("img", { name: en.packageRegistries.gitCredentialHelp }).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("img", { name: en.packageRegistries.caBundleHelp }).length,
    ).toBeGreaterThan(0);
  });

  it("shows the server's refusal beside the entry", async () => {
    replace.mockRejectedValue(new Error("https://other.test/ is already the mirror"));
    render(<PackageRegistriesTab />);
    fireEvent.click(await screen.findByTestId("package-registry-r1-mirror"));
    fireEvent.click(screen.getByTestId("package-registry-r1-save"));
    expect(await screen.findByTestId("package-registry-r1-said")).toHaveTextContent(
      "already the mirror",
    );
  });
});
