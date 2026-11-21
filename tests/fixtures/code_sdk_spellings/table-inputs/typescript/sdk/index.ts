import { table, type Rows } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region. */
export const rows = table(function rows(inputs: Inputs): Rows<Row> {
  return [];
});
