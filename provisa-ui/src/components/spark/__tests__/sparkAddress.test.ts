// Copyright (c) 2026 Kenneth Stott
// Canary: 7c31e8d2-4a5f-4b96-9e07-a18b3f6d2c45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225 (amended 2026-11-24): the cluster's fields compose the one stored sc:// address, and a
// pasted address splits back into them, a token in it held apart to be kept as a secret.

import { describe, expect, it } from "vitest";
import {
  clusterProblems,
  composeAddress,
  emptyCluster,
  NotSparkAddress,
  splitAddress,
  storedFields,
  tokenSecretName,
} from "../sparkAddress";

const reason = (address: string) => {
  try {
    splitAddress(address);
  } catch (e) {
    if (e instanceof NotSparkAddress) return e.reason;
    throw e;
  }
  return null;
};

describe("the cluster's fields", () => {
  it("start with Spark Connect's own port and nothing else", () => {
    expect(emptyCluster()).toEqual({
      host: "",
      port: "15002",
      tls: false,
      userId: "",
      token: "",
      extra: [],
    });
  });

  it("compose no address while the host is blank: the cluster is left unset", () => {
    expect(composeAddress({ ...emptyCluster(), userId: "ada" }, true)).toBe("");
  });

  it("compose an address of the host and port alone", () => {
    expect(composeAddress({ ...emptyCluster(), host: "spark.acme.test" }, true)).toBe(
      "sc://spark.acme.test:15002",
    );
  });

  it("compose TLS, the user id and the token reference as parameters", () => {
    const fields = {
      ...emptyCluster(),
      host: "spark.acme.test",
      port: "443",
      tls: true,
      userId: "ada",
      token: "${secret:spark_token}",
    };
    expect(composeAddress(fields, true)).toBe(
      "sc://spark.acme.test:443/;use_ssl=true;user_id=ada;token=${secret:spark_token}",
    );
    // The organisation's default keeps its token beside the address.
    expect(composeAddress(fields, false)).toBe(
      "sc://spark.acme.test:443/;use_ssl=true;user_id=ada",
    );
  });

  it("turn TLS on with a token, which Spark Connect sends over TLS only", () => {
    const fields = { ...emptyCluster(), host: "h", token: "${secret:t}" };
    expect(composeAddress(fields, true)).toBe("sc://h:15002/;use_ssl=true;token=${secret:t}");
  });

  it("round-trip a stored address", () => {
    const stored = "sc://spark.acme.test:443/;use_ssl=true;user_id=ada;token=${secret:spark_token}";
    expect(composeAddress(storedFields(stored), true)).toBe(stored);
    const other = "sc://[::1]:15999/;session_id=abc";
    expect(composeAddress(storedFields(other), true)).toBe(other);
  });

  it("name what is wrong, by field", () => {
    expect(clusterProblems({ ...emptyCluster(), host: "sc://x", port: "0" })).toEqual({
      host: "host",
      port: "port",
    });
    expect(clusterProblems({ ...emptyCluster(), host: "x", token: "abc" })).toEqual({
      token: "token",
    });
    expect(clusterProblems(emptyCluster())).toEqual({});
  });
});

describe("a pasted address", () => {
  it("splits into the fields", () => {
    const { fields, tokenValue } = splitAddress(
      " sc://spark.acme.test:443/;use_ssl=true;user_id=ada ",
    );
    expect(fields).toEqual({
      host: "spark.acme.test",
      port: "443",
      tls: true,
      userId: "ada",
      token: "",
      extra: [],
    });
    expect(tokenValue).toBeNull();
  });

  it("with no port takes Spark Connect's own", () => {
    expect(splitAddress("sc://spark.acme.test").fields.port).toBe("15002");
  });

  it("holds a token value apart, never in a field", () => {
    const { fields, tokenValue } = splitAddress("sc://spark.acme.test/;token=abc123");
    expect(tokenValue).toBe("abc123");
    expect(fields.token).toBe("");
    expect(JSON.stringify(fields)).not.toContain("abc123");
  });

  it("keeps a token that is a secret reference already", () => {
    const { fields, tokenValue } = splitAddress("sc://h/;token=${secret:spark_token}");
    expect(tokenValue).toBeNull();
    expect(fields.token).toBe("${secret:spark_token}");
  });

  it("is refused by name when it is not a Spark Connect address", () => {
    expect(reason("spark.acme.test:15002")).toBe("scheme");
    expect(reason("sc://user:pw@spark.acme.test")).toBe("host");
    expect(reason("sc://:15002")).toBe("host");
    expect(reason("sc://spark.acme.test:99999")).toBe("port");
    expect(reason("sc://spark.acme.test/;use_ssl")).toBe("param");
  });

  it("names a secret for its token after its host", () => {
    expect(tokenSecretName("spark.acme.test")).toBe("spark_spark_acme_test_token");
  });
});
