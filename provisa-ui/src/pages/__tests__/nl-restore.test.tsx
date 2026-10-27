// Copyright (c) 2026 Kenneth Stott
// Canary: 44c0ece3-7777-442a-9a13-305e6ae481b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The NL page keeps its answers with the question that produced them: opened again, it shows them
// only beside that question, never beside a question edited since.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import { setStored } from "../../lib/browserStorage";
import { kept } from "../../lib/resultWithQuery";

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({ role: { id: "analyst" } }),
}));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchNlMode: async () => ({ non_strict: false }),
}));

import { NlPage } from "../NlPage";

const EMPTY = { query: null, result: null, error: null, loading: false };
const ANSWERS = {
  sql: { query: "SELECT count(*) FROM owners", result: null, error: null, loading: false },
  graphql: EMPTY,
  cypher: EMPTY,
  grpc: EMPTY,
  jsonapi: EMPTY,
  openapi: EMPTY,
};

describe("the NL page's kept answers", () => {
  beforeEach(() => localStorage.clear());

  it("are shown beside the question that produced them", async () => {
    setStored("nl-question", "how many owners");
    setStored("nl-branches", JSON.stringify(kept("how many owners", ANSWERS)));
    render(<NlPage />);
    expect(await screen.findByText("SELECT count(*) FROM owners")).toBeInTheDocument();
  });

  it("are not shown beside a question edited since", async () => {
    setStored("nl-question", "how many pets");
    setStored("nl-branches", JSON.stringify(kept("how many owners", ANSWERS)));
    render(<NlPage />);
    await waitFor(() => expect(screen.getByDisplayValue("how many pets")).toBeInTheDocument());
    expect(screen.queryByText("SELECT count(*) FROM owners")).not.toBeInTheDocument();
  });
});
