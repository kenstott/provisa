// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d9e47-3c82-4f05-b7e6-d0c4a2f8b913
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225: Admin › Spark cluster holds the organisation's default Spark cluster: a Spark Connect
// address and a token secret reference, with a connection test. It says which cluster a source
// naming none uses now: the default, the local test cluster of a demo install, or none.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";

const auth = vi.hoisted(() => ({
  capabilities: ["org_settings"] as string[],
  activeOrgId: "acme" as string | null,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../api/sparkCluster", () => ({
  fetchSparkCluster: vi.fn(),
  saveSparkCluster: vi.fn(),
  deleteSparkCluster: vi.fn(),
  testSparkCluster: vi.fn(),
}));

import {
  deleteSparkCluster,
  fetchSparkCluster,
  saveSparkCluster,
  testSparkCluster,
} from "../api/sparkCluster";
import { SparkClusterTab } from "../components/admin/SparkClusterTab";
import { NAV_GROUPS } from "../components/navGroups";

const fetchIt = vi.mocked(fetchSparkCluster);
const save = vi.mocked(saveSparkCluster);
const remove = vi.mocked(deleteSparkCluster);
const test = vi.mocked(testSparkCluster);

const KEPT = {
  cluster: { address: "sc://spark.acme.test:15002", credential: "${secret:spark_token}" },
  in_use: { origin: "organisation" as const, label: null, target: "spark.acme.test:15002" },
};
const LOCAL = {
  cluster: null,
  in_use: { origin: "local_test" as const, label: "local test cluster", target: "127.0.0.1:15002" },
};

beforeEach(() => {
  fetchIt.mockReset().mockResolvedValue(KEPT);
  save.mockReset().mockResolvedValue(KEPT);
  remove.mockReset().mockResolvedValue(undefined);
  test.mockReset().mockResolvedValue({
    ok: true,
    spark_version: "4.3.0",
    target: "spark.acme.test:15002",
  });
});

describe("Admin › Spark cluster", () => {
  it("is an organisation setting in the admin navigation", () => {
    const item = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/spark-cluster");
    expect(item?.capability).toBe("org_settings");
  });

  it("shows the default as entered, in its fields, and that sources naming none use it", async () => {
    render(<SparkClusterTab />);
    // REQ-2225 (amended 2026-11-24): the address is entered as structured fields.
    const host = (await screen.findByTestId("spark-cluster-host")) as HTMLInputElement;
    expect(host.value).toBe("spark.acme.test");
    expect((screen.getByTestId("spark-cluster-port") as HTMLInputElement).value).toBe("15002");
    expect((screen.getByTestId("spark-cluster-credential") as HTMLInputElement).value).toBe(
      "${secret:spark_token}",
    );
    expect(screen.getByTestId("spark-cluster-in-use")).toHaveTextContent("spark.acme.test:15002");
  });

  it("says a demo install runs on its local test cluster until a default is set", async () => {
    fetchIt.mockResolvedValue(LOCAL);
    render(<SparkClusterTab />);
    expect(await screen.findByTestId("spark-cluster-in-use")).toHaveTextContent(
      /local test cluster/,
    );
    expect(screen.getByTestId("spark-cluster-save")).toBeDisabled();
    expect(screen.queryByTestId("spark-cluster-remove")).toBeNull();
  });

  it("says when no cluster is configured", async () => {
    fetchIt.mockResolvedValue({ cluster: null, in_use: { origin: null, detail: "none" } });
    render(<SparkClusterTab />);
    expect(await screen.findByTestId("spark-cluster-in-use")).toHaveTextContent(
      /No Spark cluster is configured/,
    );
  });

  it("saves the address its fields compose and the token reference, an empty token as none", async () => {
    render(<SparkClusterTab />);
    fireEvent.change(await screen.findByTestId("spark-cluster-host"), {
      target: { value: "other.acme.test" },
    });
    fireEvent.change(screen.getByTestId("spark-cluster-port"), { target: { value: "443" } });
    // A token turns TLS on; without one the switch is the administrator's.
    expect(screen.getByTestId("spark-cluster-tls")).toBeDisabled();
    fireEvent.change(screen.getByTestId("spark-cluster-credential"), { target: { value: "" } });
    fireEvent.click(screen.getByTestId("spark-cluster-tls"));
    fireEvent.change(screen.getByTestId("spark-cluster-user"), { target: { value: "ada" } });
    fireEvent.click(screen.getByTestId("spark-cluster-save"));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith("acme", {
        address: "sc://other.acme.test:443/;use_ssl=true;user_id=ada",
        credential: null,
      }),
    );
  });

  it("keeps the token beside the address, never in it", async () => {
    render(<SparkClusterTab />);
    fireEvent.click(await screen.findByTestId("spark-cluster-save"));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith("acme", {
        address: "sc://spark.acme.test:15002",
        credential: "${secret:spark_token}",
      }),
    );
  });

  it("is not saved while a field is wrong", async () => {
    render(<SparkClusterTab />);
    fireEvent.change(await screen.findByTestId("spark-cluster-port"), { target: { value: "0" } });
    expect(screen.getByTestId("spark-cluster-save")).toBeDisabled();
    expect(screen.getByText("A port from 1 to 65535")).toBeInTheDocument();
  });

  it("splits a pasted address into its fields", async () => {
    render(<SparkClusterTab />);
    fireEvent.click(await screen.findByTestId("spark-cluster-paste"));
    fireEvent.change(await screen.findByTestId("spark-cluster-paste-input"), {
      target: { value: "sc://pasted.acme.test:16000/;user_id=bo" },
    });
    fireEvent.click(screen.getByTestId("spark-cluster-paste-apply"));
    await waitFor(() =>
      expect((screen.getByTestId("spark-cluster-host") as HTMLInputElement).value).toBe(
        "pasted.acme.test",
      ),
    );
    expect((screen.getByTestId("spark-cluster-port") as HTMLInputElement).value).toBe("16000");
    expect((screen.getByTestId("spark-cluster-user") as HTMLInputElement).value).toBe("bo");
  });

  it("tests the connection as entered and shows the Spark version", async () => {
    render(<SparkClusterTab />);
    fireEvent.click(await screen.findByTestId("spark-cluster-test"));
    await waitFor(() =>
      expect(test).toHaveBeenCalledWith("acme", {
        address: "sc://spark.acme.test:15002",
        credential: "${secret:spark_token}",
      }),
    );
    expect(await screen.findByTestId("spark-cluster-said")).toHaveTextContent("Spark 4.3.0");
  });

  it("shows the server's words when the test fails", async () => {
    test.mockRejectedValue(new Error("The Spark cluster at x:1 did not answer: UNAVAILABLE"));
    render(<SparkClusterTab />);
    fireEvent.click(await screen.findByTestId("spark-cluster-test"));
    expect(await screen.findByTestId("spark-cluster-said")).toHaveTextContent("UNAVAILABLE");
  });

  it("removes the default", async () => {
    render(<SparkClusterTab />);
    fireEvent.click(await screen.findByTestId("spark-cluster-remove"));
    await waitFor(() => expect(remove).toHaveBeenCalledWith("acme"));
  });
});
