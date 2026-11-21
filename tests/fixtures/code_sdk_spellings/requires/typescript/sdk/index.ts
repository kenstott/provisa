import { table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows from the API. */
export const rows = table(
  function rows(): Row[] {
    return [];
  },
  { requires: ["API_KEY:secret", "REGION"] },
);
