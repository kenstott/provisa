// Copyright (c) 2026 Kenneth Stott
// Canary: 95511fd2-ea41-4d1d-b78e-c9697f2f727b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: Admin › Container registries holds where a code source's built image is pushed. Each
// field is shown again as it was entered, a new entry starts empty, and an entry is not saved
// until it has its address, user name and credential.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { expectFilterFindsEveryCell } from "../test-utils/listFilter";

const auth = vi.hoisted(() => ({
  capabilities: ["org_settings"] as string[],
  activeOrgId: "acme" as string | null,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../api/containerRegistries", () => ({
  fetchContainerRegistries: vi.fn(),
  addContainerRegistry: vi.fn(),
  replaceContainerRegistry: vi.fn(),
  deleteContainerRegistry: vi.fn(),
}));

import {
  addContainerRegistry,
  deleteContainerRegistry,
  fetchContainerRegistries,
  replaceContainerRegistry,
} from "../api/containerRegistries";
import { ContainerRegistriesTab } from "../components/admin/ContainerRegistriesTab";
import { NAV_GROUPS } from "../components/navGroups";

const list = vi.mocked(fetchContainerRegistries);
const add = vi.mocked(addContainerRegistry);
const replace = vi.mocked(replaceContainerRegistry);
const remove = vi.mocked(deleteContainerRegistry);

const GHCR = {
  id: "r1",
  address: "ghcr.io/acme",
  username: "acme-bot",
  credential: "${secret:ghcr_acme}",
  default: true,
  ca_bundle: null,
};

beforeEach(() => {
  list.mockReset().mockResolvedValue([GHCR]);
  add.mockReset().mockResolvedValue(GHCR);
  replace.mockReset().mockResolvedValue(GHCR);
  remove.mockReset().mockResolvedValue(undefined);
});

// A panel opened in one test is remembered (REQ-2260); each test starts from none.
beforeEach(() => localStorage.clear());

describe("Admin › Container registries", () => {
  it("is an organisation setting in the admin navigation", () => {
    const item = NAV_GROUPS.flatMap((g) => g.items).find(
      (i) => i.to === "/admin/container-registries",
    );
    expect(item?.capability).toBe("org_settings");
  });

  it("finds each entry by every text its row shows", async () => {
    list.mockResolvedValue([
      GHCR,
      {
        ...GHCR,
        id: "r2",
        address: "registry.internal:5000/data",
        username: "pusher",
        default: false,
      },
    ]);
    render(<ContainerRegistriesTab title="Container Registries" />);
    await screen.findByTestId("container-registry-r2");
    await expectFilterFindsEveryCell(screen.getByTestId("container-registries-filter"), () =>
      screen.getByTestId("container-registries-list"),
    );
  });

  it("says nothing matches when the filter empties the list", async () => {
    render(<ContainerRegistriesTab title="Container Registries" />);
    await screen.findByTestId("container-registry-r1");
    fireEvent.change(screen.getByTestId("container-registries-filter"), {
      target: { value: "no-such-registry" },
    });
    expect(screen.getByTestId("container-registries-empty")).toHaveTextContent(
      "No container registries match the filter.",
    );
  });

  it("lists each entry, a row opening its fields as they were entered", async () => {
    render(<ContainerRegistriesTab title="Container Registries" />);
    fireEvent.click(await screen.findByTestId("container-registry-r1"));
    const read = await screen.findByTestId("container-registry-read-r1");
    expect(read).toHaveTextContent("ghcr.io/acme");
    expect(read).toHaveTextContent("acme-bot");
    fireEvent.click(screen.getByTestId("container-registry-edit-r1"));
    expect(screen.getByTestId("container-registry-address")).toHaveValue("ghcr.io/acme");
    expect(screen.getByTestId("container-registry-username")).toHaveValue("acme-bot");
    expect(screen.getByTestId("container-registry-credential")).toHaveValue("${secret:ghcr_acme}");
    expect(screen.getByTestId("container-registry-default")).toBeChecked();
  });

  it("adds an entry, checking its address, user name and credential on save", async () => {
    list.mockResolvedValue([]);
    render(<ContainerRegistriesTab title="Container Registries" />);
    expect(await screen.findByText(/No container registries/)).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("container-registries-add"));
    fireEvent.change(await screen.findByTestId("container-registry-address"), {
      target: { value: "quay.io/acme" },
    });
    fireEvent.change(screen.getByTestId("container-registry-username"), {
      target: { value: "bot" },
    });
    // REQ-2277: Save is offered and names the missing credential when pressed.
    fireEvent.click(screen.getByTestId("container-registry-save"));
    expect(await screen.findByText("Required")).toBeInTheDocument();
    expect(add).not.toHaveBeenCalled();
    fireEvent.change(screen.getByTestId("container-registry-credential"), {
      target: { value: "${secret:quay}" },
    });
    fireEvent.click(screen.getByTestId("container-registry-save"));
    await waitFor(() =>
      expect(add).toHaveBeenCalledWith("acme", {
        address: "quay.io/acme",
        username: "bot",
        credential: "${secret:quay}",
        default: false,
        ca_bundle: null,
      }),
    );
  });

  it("replaces and removes a kept entry", async () => {
    render(<ContainerRegistriesTab title="Container Registries" />);
    fireEvent.click(await screen.findByTestId("container-registry-r1"));
    fireEvent.click(await screen.findByTestId("container-registry-edit-r1"));
    fireEvent.change(screen.getByTestId("container-registry-username"), {
      target: { value: "other" },
    });
    fireEvent.click(screen.getByTestId("container-registry-save"));
    await waitFor(() =>
      expect(replace).toHaveBeenCalledWith(
        "acme",
        "r1",
        expect.objectContaining({ username: "other" }),
      ),
    );
    fireEvent.click(await screen.findByTestId("container-registry-remove-r1"));
    fireEvent.click(
      within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }),
    );
    await waitFor(() => expect(remove).toHaveBeenCalledWith("acme", "r1"));
  });
});
