// Copyright (c) 2026 Kenneth Stott
// Canary: e4a07b93-6d2c-4f18-b5e1-930c7d28af64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225 (amended 2026-11-24): a Spark source's own cluster as structured fields composing its
// one address; Paste address splits a pasted address into them and keeps a token as a secret; a
// blank cluster is the organisation's default, which the form names.

import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
vi.mock("../../../api/secrets", () => ({ putSecret: vi.fn() }));
vi.mock("../../../api/sparkCluster", () => ({ fetchSparkSource: vi.fn() }));

import { putSecret } from "../../../api/secrets";
import { fetchSparkSource } from "../../../api/sparkCluster";
import { SparkSourceCluster } from "../SparkClusterFields";

const put = vi.mocked(putSecret);
const fetchSource = vi.mocked(fetchSparkSource);

function Held({ start = "", seen }: { start?: string; seen: (address: string) => void }) {
  const [value, setValue] = useState(start);
  return (
    <SparkSourceCluster
      value={value}
      onChange={(address) => {
        setValue(address);
        seen(address);
      }}
    />
  );
}

const field = (id: string) => screen.getByTestId(`spark-source-cluster-${id}`) as HTMLInputElement;

beforeEach(() => {
  put.mockReset().mockResolvedValue({} as never);
  fetchSource.mockReset().mockResolvedValue({
    host: {} as never,
    default_cluster: { origin: "organisation", label: null, target: "spark.acme.test:15002" },
  });
});

describe("a Spark source's cluster", () => {
  it("starts blank on port 15002 and names the organisation's default it then runs on", async () => {
    renderInForm(<Held seen={() => undefined} />);
    expect(field("host").value).toBe("");
    expect(field("port").value).toBe("15002");
    expect(await screen.findByTestId("spark-source-cluster-default")).toHaveTextContent(
      "organization's default Spark cluster (spark.acme.test:15002)",
    );
    expect(fetchSource).toHaveBeenCalledWith("acme");
  });

  it("names the local test cluster when that is the default", async () => {
    fetchSource.mockResolvedValue({
      host: {} as never,
      default_cluster: { origin: "local_test", label: "local test cluster", target: "127.0.0.1" },
    });
    renderInForm(<Held seen={() => undefined} />);
    expect(await screen.findByTestId("spark-source-cluster-default")).toHaveTextContent(
      "local test cluster (127.0.0.1)",
    );
  });

  it("composes one address from its fields", () => {
    const seen = vi.fn();
    renderInForm(<Held seen={seen} />);
    fireEvent.change(field("host"), { target: { value: "spark.acme.test" } });
    fireEvent.change(field("port"), { target: { value: "443" } });
    fireEvent.click(field("tls"));
    fireEvent.change(field("user"), { target: { value: "ada" } });
    fireEvent.change(field("credential"), { target: { value: "${secret:spark_token}" } });
    expect(seen).toHaveBeenLastCalledWith(
      "sc://spark.acme.test:443/;use_ssl=true;user_id=ada;token=${secret:spark_token}",
    );
    expect(screen.queryByTestId("spark-source-cluster-default")).toBeNull();
  });

  it("shows a stored address in its fields", () => {
    renderInForm(
      <Held
        start="sc://own.acme.test:443/;use_ssl=true;token=${secret:t}"
        seen={() => undefined}
      />,
    );
    expect(field("host").value).toBe("own.acme.test");
    expect(field("port").value).toBe("443");
    expect(field("tls").checked).toBe(true);
    expect(field("credential").value).toBe("${secret:t}");
  });

  it("says a token that is not a secret reference is wrong", () => {
    renderInForm(<Held start="sc://own.acme.test" seen={() => undefined} />);
    fireEvent.change(field("credential"), { target: { value: "abc123" } });
    expect(screen.getByText(/A secret reference, such as/)).toBeInTheDocument();
  });

  it("splits a pasted address into its fields", async () => {
    const seen = vi.fn();
    renderInForm(<Held seen={seen} />);
    fireEvent.click(field("paste"));
    fireEvent.change(await screen.findByTestId("spark-source-cluster-paste-input"), {
      target: { value: "sc://pasted.acme.test:16000/;use_ssl=true;user_id=bo" },
    });
    fireEvent.click(screen.getByTestId("spark-source-cluster-paste-apply"));
    await waitFor(() => expect(field("host").value).toBe("pasted.acme.test"));
    expect(field("port").value).toBe("16000");
    expect(field("user").value).toBe("bo");
    expect(seen).toHaveBeenLastCalledWith("sc://pasted.acme.test:16000/;use_ssl=true;user_id=bo");
    expect(put).not.toHaveBeenCalled();
  });

  it("moves a pasted token into a secret and names that secret", async () => {
    const seen = vi.fn();
    renderInForm(<Held seen={seen} />);
    fireEvent.click(field("paste"));
    fireEvent.change(await screen.findByTestId("spark-source-cluster-paste-input"), {
      target: { value: "sc://pasted.acme.test/;token=abc123" },
    });
    fireEvent.click(screen.getByTestId("spark-source-cluster-paste-apply"));
    const name = (await screen.findByTestId(
      "spark-source-cluster-paste-secret",
    )) as HTMLInputElement;
    expect(name.value).toBe("spark_pasted_acme_test_token");
    fireEvent.change(name, { target: { value: "pasted_token" } });
    fireEvent.click(screen.getByTestId("spark-source-cluster-paste-apply"));
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith("acme", "org", "pasted_token", { value: "abc123" }),
    );
    await waitFor(() => expect(field("credential").value).toBe("${secret:pasted_token}"));
    expect(seen).toHaveBeenLastCalledWith(
      "sc://pasted.acme.test:15002/;use_ssl=true;token=${secret:pasted_token}",
    );
    expect(seen.mock.calls.flat().join(" ")).not.toContain("abc123");
  });

  it("refuses a pasted address that is not one, by name", async () => {
    renderInForm(<Held seen={() => undefined} />);
    fireEvent.click(field("paste"));
    fireEvent.change(await screen.findByTestId("spark-source-cluster-paste-input"), {
      target: { value: "https://spark.acme.test" },
    });
    fireEvent.click(screen.getByTestId("spark-source-cluster-paste-apply"));
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      "https://spark.acme.test is not a Spark Connect address",
    );
    expect(field("host").value).toBe("");
  });
});
