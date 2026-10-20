// Copyright (c) 2026 Kenneth Stott
// Canary: eb5a4084-c0c7-4155-a57d-3ecd5b489e7e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a saved code source's Remote host panel: build its image (pushed or downloaded), the
// compose service to run it, connect the remote host (the source is then built, and an edit of
// the form carries that through), the host's status and re-pinning to the code it reports.

import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";

vi.mock("../../../api/containerRegistries", () => ({
  fetchContainerRegistries: vi.fn(),
}));
vi.mock("../../../api/codeHosts", () => ({
  startBuild: vi.fn(),
  fetchBuild: vi.fn(),
  fetchBuilds: vi.fn(),
  archiveUrl: (o: string, s: string, b: string) => `/archive/${o}/${s}/${b}`,
  connectHost: vi.fn(),
  backToDev: vi.fn(),
  fetchHostStatus: vi.fn(),
  repin: vi.fn(),
}));

import { fetchContainerRegistries } from "../../../api/containerRegistries";
import {
  backToDev,
  connectHost,
  fetchBuilds,
  fetchHostStatus,
  repin,
  startBuild,
} from "../../../api/codeHosts";
import { CodeHostPanel } from "../CodeHostPanel";
import { codeFieldsFromMapping, codeMappingJson } from "../codeSource";

const DONE = {
  id: "b1",
  source_id: "rates",
  code_hash: "h".repeat(64),
  arch: "amd64" as const,
  registry_id: "r1",
  status: "done" as const,
  image_ref: "ghcr.io/acme/provisa-code-rates:hhhh-0.1.0",
  digest: "sha256:d",
  node: null,
  error: null,
  started_at: "2026-10-20T00:00:00Z",
  finished_at: "2026-10-20T00:03:00Z",
  compose:
    'services:\n  provisa-code-rates:\n    ports:\n      - "${PROVISA_HOST_PORT:?set it}:7443"\n',
};

function Harness({
  initial,
  seen,
}: {
  initial: Record<string, string>;
  seen: Record<string, string>[];
}) {
  const [fields, setFields] = useState(initial);
  return (
    <CodeHostPanel
      orgId="acme"
      sourceId="rates"
      fields={fields}
      setFields={(f) => {
        seen.push(f);
        setFields(f);
      }}
    />
  );
}

const DEV = { code_mode: "dev", code_content_hash: "h".repeat(64) };

beforeEach(() => {
  vi.mocked(fetchContainerRegistries)
    .mockReset()
    .mockResolvedValue([
      {
        id: "r1",
        address: "ghcr.io/acme",
        username: "bot",
        credential: "x",
        default: true,
        ca_bundle: null,
      },
    ]);
  vi.mocked(fetchBuilds).mockReset().mockResolvedValue([]);
  vi.mocked(startBuild).mockReset().mockResolvedValue(DONE);
  // Once built, the panel asks how its host is.
  vi.mocked(fetchHostStatus).mockReset().mockResolvedValue({ mode: "built", answering: true });
});

describe("a dev-mode source's remote host", () => {
  it("builds its image to the default registry and shows the compose to run it", async () => {
    renderInForm(<Harness initial={DEV} seen={[]} />);
    fireEvent.click(await screen.findByTestId("code-host-build"));
    await waitFor(() =>
      expect(startBuild).toHaveBeenCalledWith("acme", "rates", {
        arch: "amd64",
        registry_id: "r1",
        download: false,
      }),
    );
    expect((await screen.findByTestId("code-host-compose")).textContent).toContain(
      "PROVISA_HOST_PORT:?",
    );
  });

  it("connects the remote host, and the source is then built", async () => {
    vi.mocked(connectHost).mockResolvedValue({
      mode: "built",
      remote: { host: "gpu", port: 9443, pin: "f".repeat(64) },
      content_hash: "h".repeat(64),
    });
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness initial={DEV} seen={seen} />);
    const connect = await screen.findByTestId("code-host-connect");
    expect(connect).toBeDisabled();
    fireEvent.change(screen.getByTestId("code-host-host"), { target: { value: "gpu" } });
    fireEvent.change(screen.getByTestId("code-host-port"), { target: { value: "9443" } });
    fireEvent.change(screen.getByTestId("code-host-pairing"), { target: { value: "abc.def" } });
    fireEvent.click(connect);
    await waitFor(() =>
      expect(connectHost).toHaveBeenCalledWith("acme", "rates", {
        host: "gpu",
        port: 9443,
        pairing_code: "abc.def",
      }),
    );
    await waitFor(() => expect(seen.at(-1)?.code_mode).toBe("built"));
    // An edit of the form saves the source as built, with its host and pin.
    const mapping = JSON.parse(
      codeMappingJson("python", { code_package: "a", code_version: "1", ...seen.at(-1) }),
    );
    expect(mapping.mode).toBe("built");
    expect(mapping.remote).toEqual({ host: "gpu", port: 9443, pin: "f".repeat(64) });
    expect(mapping.content_hash).toBe("h".repeat(64));
    expect(codeFieldsFromMapping(JSON.stringify(mapping)).code_mode).toBe("built");
  });
});

describe("a built source's remote host", () => {
  const BUILT = {
    code_mode: "built",
    code_remote: JSON.stringify({ host: "gpu", port: 9443 }),
    code_content_hash: "a".repeat(64),
  };

  it("offers a re-pin when its host runs other code, and goes back to dev", async () => {
    vi.mocked(fetchHostStatus).mockResolvedValue({
      mode: "built",
      answering: true,
      code_hash: "b".repeat(64),
    });
    vi.mocked(repin).mockResolvedValue({ content_hash: "b".repeat(64) });
    vi.mocked(backToDev).mockResolvedValue(undefined);
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness initial={BUILT} seen={seen} />);
    expect(await screen.findByTestId("code-host-other-code")).toHaveTextContent("bbbbbbbbbbbb");
    fireEvent.click(screen.getByTestId("code-host-repin"));
    await waitFor(() => expect(repin).toHaveBeenCalledWith("acme", "rates", "b".repeat(64)));
    await waitFor(() => expect(seen.at(-1)?.code_content_hash).toBe("b".repeat(64)));
    fireEvent.click(screen.getByTestId("code-host-to-dev"));
    await waitFor(() => expect(backToDev).toHaveBeenCalledWith("acme", "rates"));
    await waitFor(() => expect(seen.at(-1)?.code_mode).toBe("dev"));
  });
});
