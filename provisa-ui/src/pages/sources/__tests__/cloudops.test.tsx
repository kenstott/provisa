// Copyright (c) 2026 Kenneth Stott
// Canary: 5f0b9c31-2e64-4a87-b1d5-8c7e3a6f0d49
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1947: cloud inventory is in the source pick list under Enterprise; its form names one or
// more clouds, each all or nothing, and saves only what is filled.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { CloudopsFields } from "../CloudopsFields";
import { SOURCE_TYPES } from "../constants";
import {
  cloudopsFieldsFromMapping,
  cloudopsMappingJson,
  cloudopsMissing,
  cloudopsNamedClouds,
} from "../cloudops";

const AWS = {
  aws_access_key_id: "AKIA1",
  aws_secret_access_key: "${secret:aws}",
  aws_region: "us-east-1",
  aws_account_ids: "111111111111",
};

describe("the source pick list", () => {
  it("offers cloud inventory under Enterprise", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "cloudops")).toMatchObject({
      label: "Cloud Inventory (Azure / AWS / GCP)",
      category: "Enterprise",
    });
  });
});

describe("cloudops helpers", () => {
  it("names the clouds whose fields are filled", () => {
    expect(cloudopsNamedClouds({})).toEqual([]);
    expect(cloudopsNamedClouds({ ...AWS, gcp_project_ids: "p" })).toEqual(["aws", "gcp"]);
  });

  it("an optional field alone names no cloud", () => {
    expect(cloudopsNamedClouds({ aws_role_arn: "arn:role" })).toEqual([]);
  });

  it("lists what a partly named cloud still needs", () => {
    expect(cloudopsMissing(AWS)).toEqual([]);
    expect(cloudopsMissing({ azure_tenant_id: "t" })).toEqual([
      "azure_client_id",
      "azure_client_secret",
      "azure_subscription_ids",
    ]);
  });

  it("saves only what is filled and reads it back", () => {
    const fields = { ...AWS, aws_role_arn: " ", cache_ttl_minutes: "30" };
    const mapping = JSON.parse(cloudopsMappingJson(fields));
    expect(mapping).toEqual({ ...AWS, cache_ttl_minutes: "30" });
    expect(cloudopsFieldsFromMapping(JSON.stringify(mapping))).toEqual({
      ...AWS,
      cache_ttl_minutes: "30",
    });
  });

  it("reads a numeric cache TTL back as text", () => {
    expect(cloudopsFieldsFromMapping('{"cache_ttl_minutes": 30}')).toEqual({
      cache_ttl_minutes: "30",
    });
  });
});

describe("CloudopsFields", () => {
  const show = (fields: Record<string, string>) =>
    render(<CloudopsFields fields={fields} setFields={vi.fn()} />);

  it("shows every cloud's fields and asks for a cloud when none is named", () => {
    show({});
    for (const key of ["azure_tenant_id", "aws_access_key_id", "gcp_credentials_path"]) {
      expect(screen.getByTestId(`cloudops-${key}`)).toBeInTheDocument();
    }
    expect(screen.getByTestId("cloudops-clouds-note")).toHaveTextContent(/at least one/i);
  });

  it("says which clouds the source names", () => {
    show(AWS);
    expect(screen.getByTestId("cloudops-clouds-note")).toHaveTextContent("AWS");
  });

  it("marks what a partly named cloud still needs", () => {
    show({ azure_tenant_id: "t" });
    expect(screen.getByTestId("cloudops-azure_client_id")).toBeInvalid();
    expect(screen.getByTestId("cloudops-aws_access_key_id")).not.toBeInvalid();
  });
});
