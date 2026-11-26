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
// REQ-2200: Go module proxies are a fourth list, beside npm, PyPI and Git.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
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

// A panel opened in one test is remembered (REQ-2260); each test starts from none.
beforeEach(() => localStorage.clear());

describe("Admin › Package registries", () => {
  it("is an organisation setting in the admin navigation", () => {
    const items = NAV_GROUPS.flatMap((g) => g.items);
    const item = items.find((i) => i.to === "/admin/package-registries");
    expect(item?.capability).toBe("org_settings");
  });

  it("lists every kind's entries in one list, a row opening the entry as entered", async () => {
    render(<PackageRegistriesTab title="Package Registries" />);
    const row = await screen.findByTestId("package-registry-r1");
    expect(row).toHaveTextContent(en.packageRegistries.kind_npm);
    expect(screen.getByTestId("package-registry-g1")).toHaveTextContent(
      en.packageRegistries.kind_git,
    );
    fireEvent.click(row);
    const read = await screen.findByTestId("package-registry-read-r1");
    expect(read).toHaveTextContent(NPM.address);
    expect(read).toHaveTextContent("@acme");
    fireEvent.click(screen.getByTestId("package-registry-g1"));
    // A Git host has no scope and no mirror.
    const git = await screen.findByTestId("package-registry-read-g1");
    expect(git).not.toHaveTextContent(en.packageRegistries.scope);
    expect(git).not.toHaveTextContent(en.packageRegistries.mirror);
  });

  it("says the public registries are used when none is entered", async () => {
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab title="Package Registries" />);
    expect(await screen.findByText(en.packageRegistries.none)).toBeInTheDocument();
  });

  it("adds an npm mirror, checking its address and credential on save", async () => {
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registries-add"));
    // REQ-2277: Save is offered; the required fields are checked when it is pressed.
    fireEvent.click(await screen.findByTestId("package-registry-save"));
    expect((await screen.findAllByText("Required")).length).toBe(2);
    expect(add).not.toHaveBeenCalled();
    fireEvent.change(screen.getByTestId("package-registry-address"), {
      target: { value: "https://mirror.acme.test/" },
    });
    fireEvent.change(screen.getByTestId("package-registry-credential"), {
      target: { value: "tok" },
    });
    fireEvent.click(screen.getByTestId("package-registry-mirror"));
    fireEvent.click(screen.getByTestId("package-registry-save"));
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

  /** Opens the create form on `kind`. */
  const createKind = async (label: string) => {
    fireEvent.click(await screen.findByTestId("package-registries-add"));
    fireEvent.click(await screen.findByTestId("package-registry-kind"));
    fireEvent.click(await screen.findByRole("option", { name: label, hidden: true }));
  };

  it("adds a PyPI index with no scope, which may be the default index", async () => {
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab title="Package Registries" />);
    await createKind(en.packageRegistries.kind_pypi);
    expect(screen.queryByTestId("package-registry-scope")).toBeNull();
    fireEvent.change(screen.getByTestId("package-registry-address"), {
      target: { value: "https://pypi.acme.test/simple/" },
    });
    fireEvent.change(screen.getByTestId("package-registry-credential"), {
      target: { value: "tok" },
    });
    fireEvent.click(screen.getByTestId("package-registry-mirror"));
    fireEvent.click(screen.getByTestId("package-registry-save"));
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

  it("adds a Go module proxy with no scope, which may be the default proxy", async () => {
    // REQ-2200: Go modules have no scope; the credential is the go command's user:password.
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab title="Package Registries" />);
    await createKind(en.packageRegistries.kind_goproxy);
    const form = screen.getByTestId("package-registry-form");
    expect(screen.queryByTestId("package-registry-scope")).toBeNull();
    expect(form).toHaveTextContent(en.packageRegistries.goproxyCredential);
    fireEvent.change(screen.getByTestId("package-registry-address"), {
      target: { value: "https://goproxy.acme.test" },
    });
    fireEvent.change(screen.getByTestId("package-registry-credential"), {
      target: { value: "ada:made-up-proxy-token" },
    });
    fireEvent.click(screen.getByTestId("package-registry-mirror"));
    fireEvent.click(screen.getByTestId("package-registry-save"));
    await waitFor(() =>
      expect(add).toHaveBeenCalledWith("acme", {
        kind: "goproxy",
        address: "https://goproxy.acme.test",
        scope: null,
        credential: "ada:made-up-proxy-token",
        mirror: true,
        ca_bundle: null,
      }),
    );
  });

  it("adds a Cargo registry by its sparse index and token, as the default", async () => {
    // REQ-2227: crates have no scope; the credential is the token cargo sends.
    list.mockResolvedValue([]);
    render(<PackageRegistriesTab title="Package Registries" />);
    await createKind(en.packageRegistries.kind_cargo);
    const form = screen.getByTestId("package-registry-form");
    expect(screen.queryByTestId("package-registry-scope")).toBeNull();
    expect(form).toHaveTextContent(en.packageRegistries.cargoAddress);
    expect(form).toHaveTextContent(en.packageRegistries.cargoCredential);
    expect(form).toHaveTextContent(en.packageRegistries.cargoMirror);
    expect(screen.getByTestId("package-registry-address")).toHaveAttribute(
      "placeholder",
      "sparse+https://cargo.example.com/index/",
    );
    fireEvent.change(screen.getByTestId("package-registry-address"), {
      target: { value: "sparse+https://cargo.acme.test/index/" },
    });
    fireEvent.change(screen.getByTestId("package-registry-credential"), {
      target: { value: "made-up-cargo-token" },
    });
    fireEvent.click(screen.getByTestId("package-registry-mirror"));
    fireEvent.click(screen.getByTestId("package-registry-save"));
    await waitFor(() =>
      expect(add).toHaveBeenCalledWith("acme", {
        kind: "cargo",
        address: "sparse+https://cargo.acme.test/index/",
        scope: null,
        credential: "made-up-cargo-token",
        mirror: true,
        ca_bundle: null,
      }),
    );
  });

  it("replaces a kept entry by its id, from its panel's Edit", async () => {
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registry-r1"));
    fireEvent.click(await screen.findByTestId("package-registry-edit-r1"));
    expect(screen.getByTestId("package-registry-address")).toHaveValue(NPM.address);
    expect(screen.getByTestId("package-registry-scope")).toHaveValue("@acme");
    expect(screen.getByTestId("package-registry-credential")).toHaveValue(NPM.credential);
    fireEvent.change(screen.getByTestId("package-registry-address"), {
      target: { value: "https://npm2.acme.test/" },
    });
    fireEvent.click(screen.getByTestId("package-registry-save"));
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

  it("duplicates a kept entry into a new one", async () => {
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registry-g1"));
    fireEvent.click(await screen.findByTestId("package-registry-duplicate-g1"));
    expect(screen.getByTestId("package-registry-address")).toHaveValue(GIT.address);
    fireEvent.click(screen.getByTestId("package-registry-save"));
    await waitFor(() =>
      expect(add).toHaveBeenCalledWith("acme", expect.objectContaining({ kind: "git" })),
    );
    expect(replace).not.toHaveBeenCalled();
  });

  it("removes a kept entry once confirmed", async () => {
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registry-g1"));
    fireEvent.click(await screen.findByTestId("package-registry-remove-g1"));
    expect(remove).not.toHaveBeenCalled();
    fireEvent.click(
      within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }),
    );
    await waitFor(() => expect(remove).toHaveBeenCalledWith("acme", "g1"));
  });

  it("shows each entry's certificate authority as entered, and sends it on save", async () => {
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registry-g1"));
    expect(await screen.findByTestId("package-registry-read-g1")).toHaveTextContent(
      "${secret:git_ca}",
    );
    fireEvent.click(screen.getByTestId("package-registry-r1"));
    fireEvent.click(await screen.findByTestId("package-registry-edit-r1"));
    expect(screen.getByTestId("package-registry-ca-bundle")).toHaveValue("");
    const pem = "-----BEGIN CERTIFICATE-----\nMIIBmade-up\n-----END CERTIFICATE-----";
    fireEvent.change(screen.getByTestId("package-registry-ca-bundle"), {
      target: { value: pem },
    });
    fireEvent.click(screen.getByTestId("package-registry-save"));
    await waitFor(() =>
      expect(replace).toHaveBeenCalledWith(
        "acme",
        "r1",
        expect.objectContaining({ ca_bundle: pem }),
      ),
    );
  });

  it("names Artifactory and each Git provider's user in its help", async () => {
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registry-g1"));
    await screen.findByTestId("package-registry-read-g1");
    expect(en.packageRegistries.npmCredentialHelp).toMatch(/Artifactory/);
    expect(en.packageRegistries.caBundleHelp).toMatch(/Artifactory/);
    expect(en.packageRegistries.gitCredentialHelp).toMatch(/x-access-token.*GitLab.*x-token-auth/s);
    fireEvent.click(screen.getByTestId("package-registry-edit-g1"));
    // Each help is the name of its field's (?) glyph.
    expect(
      screen.getAllByRole("img", { name: en.packageRegistries.gitCredentialHelp }).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("img", { name: en.packageRegistries.caBundleHelp }).length,
    ).toBeGreaterThan(0);
  });

  it("shows the server's refusal at the form's Save", async () => {
    replace.mockRejectedValue(new Error("https://other.test/ is already the mirror"));
    render(<PackageRegistriesTab title="Package Registries" />);
    fireEvent.click(await screen.findByTestId("package-registry-r1"));
    fireEvent.click(await screen.findByTestId("package-registry-edit-r1"));
    fireEvent.click(screen.getByTestId("package-registry-mirror"));
    fireEvent.click(screen.getByTestId("package-registry-save"));
    expect(await screen.findByTestId("form-problem")).toHaveTextContent("already the mirror");
  });
});
