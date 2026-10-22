// Copyright (c) 2026 Kenneth Stott
// Canary: f5cc1174-f1ce-424f-b48f-e90236b9697b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1954: a ServiceNow source's own APIs, listed under their namespaces and switched on to be
// offered through its companion OpenAPI source.

import { describe, expect, it } from "vitest";
import { MockedProvider } from "@apollo/client/testing/react";
import { fireEvent, waitFor } from "@testing-library/react";
import { renderInForm, screen } from "../../../test-utils/render";
import { ServicenowApis, SetServicenowApi } from "../../../hooks/admin.graphql";
import { ServiceNowApisSection } from "../ServiceNowApisSection";

const api = (namespace: string, name: string, route: string, added: boolean) => ({
  __typename: "InstanceApiType",
  namespace,
  name,
  route,
  description: "",
  scripted: false,
  added,
});

const listed = (catalogAdded: boolean) => ({
  request: { query: ServicenowApis, variables: { sourceId: "helpdesk" } },
  result: {
    data: {
      servicenowApis: [
        api("now", "Import Set API", "now/import", false),
        api("sn_sc", "Service Catalog API", "sn_sc/servicecatalog", catalogAdded),
      ],
      servicenowImportSets: [
        { __typename: "CompanionImportSetType", staging: "imp_user", table: "sys_user" },
      ],
    },
  },
});

describe("ServiceNowApisSection", () => {
  it("lists the instance's APIs under their namespaces, and its import sets", async () => {
    renderInForm(
      <MockedProvider mocks={[listed(true)]}>
        <ServiceNowApisSection sourceId="helpdesk" />
      </MockedProvider>,
    );
    expect(await screen.findByText("now")).toBeInTheDocument();
    expect(screen.getByText("sn_sc")).toBeInTheDocument();
    expect(screen.getByTestId("servicenow-api-sn_sc/servicecatalog")).toBeChecked();
    expect(screen.getByTestId("servicenow-api-now/import")).not.toBeChecked();
    expect(screen.getByTestId("servicenow-import-set-imp_user")).toHaveTextContent(
      "imp_user, into sys_user",
    );
  });

  it("switching an API on offers it, and the list is read again", async () => {
    let set = false;
    const mocks = [
      listed(false),
      {
        request: {
          query: SetServicenowApi,
          variables: {
            sourceId: "helpdesk",
            namespace: "sn_sc",
            name: "Service Catalog API",
            added: true,
          },
        },
        result: () => {
          set = true;
          return {
            data: {
              setServicenowApi: {
                __typename: "MutationResult",
                success: true,
                message: "added",
                code: "schema.servicenow_api_set",
                params: {},
              },
            },
          };
        },
      },
      listed(true),
    ];
    renderInForm(
      <MockedProvider mocks={mocks}>
        <ServiceNowApisSection sourceId="helpdesk" />
      </MockedProvider>,
    );
    fireEvent.click(await screen.findByTestId("servicenow-api-sn_sc/servicecatalog"));
    await waitFor(() => expect(set).toBe(true));
    await waitFor(() =>
      expect(screen.getByTestId("servicenow-api-sn_sc/servicecatalog")).toBeChecked(),
    );
  });

  it("the filter keeps the APIs matching any word of it", async () => {
    renderInForm(
      <MockedProvider mocks={[listed(false)]}>
        <ServiceNowApisSection sourceId="helpdesk" />
      </MockedProvider>,
    );
    fireEvent.change(await screen.findByTestId("servicenow-apis-filter"), {
      target: { value: "catalog import" },
    });
    expect(screen.getByTestId("servicenow-api-now/import")).toBeInTheDocument();
    expect(screen.getByTestId("servicenow-api-sn_sc/servicecatalog")).toBeInTheDocument();
  });

  it("the filter keeps the APIs whose namespace, name or route holds it", async () => {
    renderInForm(
      <MockedProvider mocks={[listed(false)]}>
        <ServiceNowApisSection sourceId="helpdesk" />
      </MockedProvider>,
    );
    fireEvent.change(await screen.findByTestId("servicenow-apis-filter"), {
      target: { value: "catalog" },
    });
    expect(screen.queryByTestId("servicenow-api-now/import")).toBeNull();
    expect(screen.getByTestId("servicenow-api-sn_sc/servicecatalog")).toBeInTheDocument();
  });
});
