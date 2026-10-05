// Copyright (c) 2026 Kenneth Stott
// Canary: 7787dcfb-28b4-44db-91cc-cd50dd13e14e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: an Iceberg source's catalog (GLUE, REST, JDBC, SNOWFLAKE; maintainer, 2026-10-05). Each
// type shows its own fields; JDBC and Snowflake say their password sits in SingleStore's
// unredacted pipeline CONFIG; only the chosen type's fields are saved.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { IcebergCatalogFields } from "../IcebergCatalogFields";
import { ICEBERG_CATALOG_FIELDS, ICEBERG_CATALOG_TYPES, icebergCatalogHints } from "../icebergCatalog";

describe("icebergCatalogHints", () => {
  it("saves the chosen type's filled fields and none of another type's", () => {
    expect(
      icebergCatalogHints({
        iceberg_catalog_type: "REST",
        iceberg_table_id: "sales.orders",
        iceberg_catalog_uri: "http://catalog:8181",
        iceberg_catalog_warehouse: "s3://left-from-jdbc",
        access_key_id: "not a catalog field",
      }),
    ).toEqual({
      iceberg_catalog_type: "REST",
      iceberg_table_id: "sales.orders",
      iceberg_catalog_uri: "http://catalog:8181",
    });
  });

  it("saves nothing without a known catalog type", () => {
    expect(icebergCatalogHints({ iceberg_table_id: "t" })).toEqual({});
    expect(icebergCatalogHints({ iceberg_catalog_type: "HADOOP" })).toEqual({});
  });
});

describe("IcebergCatalogFields", () => {
  it.each(ICEBERG_CATALOG_TYPES)("%s shows the table id and its own catalog fields", (type) => {
    render(<IcebergCatalogFields fields={{ iceberg_catalog_type: type }} setFields={vi.fn()} />);
    expect(screen.getByTestId("iceberg-iceberg_table_id")).toBeInTheDocument();
    for (const { key } of ICEBERG_CATALOG_FIELDS[type]) {
      expect(screen.getByTestId(`iceberg-${key}`)).toBeInTheDocument();
    }
    const exposure = screen.queryByTestId("iceberg-password-exposure");
    if (type === "JDBC" || type === "SNOWFLAKE") {
      expect(exposure).toHaveTextContent(/does not redact/);
    } else {
      expect(exposure).toBeNull();
    }
  });

  it("shows only the type select before a type is chosen", () => {
    render(<IcebergCatalogFields fields={{}} setFields={vi.fn()} />);
    expect(screen.queryByTestId("iceberg-iceberg_table_id")).toBeNull();
  });
});
