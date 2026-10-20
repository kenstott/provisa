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
import { fireEvent, render, screen, waitFor } from "../test-utils/render";

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

describe("Admin › Container registries", () => {
  it("is an organisation setting in the admin navigation", () => {
    const item = NAV_GROUPS.flatMap((g) => g.items).find(
      (i) => i.to === "/admin/container-registries",
    );
    expect(item?.capability).toBe("org_settings");
  });

  it("shows each entry's fields as they were entered", async () => {
    render(<ContainerRegistriesTab />);
    const address = (await screen.findByTestId(
      "container-registry-r1-address",
    )) as HTMLInputElement;
    expect(address.value).toBe("ghcr.io/acme");
    expect((screen.getByTestId("container-registry-r1-username") as HTMLInputElement).value).toBe(
      "acme-bot",
    );
    expect((screen.getByTestId("container-registry-r1-credential") as HTMLInputElement).value).toBe(
      "${secret:ghcr_acme}",
    );
    expect((screen.getByTestId("container-registry-r1-default") as HTMLInputElement).checked).toBe(
      true,
    );
  });

  it("adds an entry, saved only once it has its address, user name and credential", async () => {
    list.mockResolvedValue([]);
    render(<ContainerRegistriesTab />);
    expect(await screen.findByTestId("container-registries-none")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("container-registries-add"));
    const card = await screen.findByTestId(/^container-registry-new-\d+$/);
    const id = card.getAttribute("data-testid") as string;
    const save = screen.getByTestId(`${id}-save`);
    expect(save).toBeDisabled();
    fireEvent.change(screen.getByTestId(`${id}-address`), { target: { value: "quay.io/acme" } });
    fireEvent.change(screen.getByTestId(`${id}-username`), { target: { value: "bot" } });
    expect(save).toBeDisabled();
    fireEvent.change(screen.getByTestId(`${id}-credential`), {
      target: { value: "${secret:quay}" },
    });
    expect(save).not.toBeDisabled();
    fireEvent.click(save);
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
    render(<ContainerRegistriesTab />);
    fireEvent.change(await screen.findByTestId("container-registry-r1-username"), {
      target: { value: "other" },
    });
    fireEvent.click(screen.getByTestId("container-registry-r1-save"));
    await waitFor(() =>
      expect(replace).toHaveBeenCalledWith(
        "acme",
        "r1",
        expect.objectContaining({ username: "other" }),
      ),
    );
    fireEvent.click(screen.getByTestId("container-registry-r1-remove"));
    await waitFor(() => expect(remove).toHaveBeenCalledWith("acme", "r1"));
  });
});
