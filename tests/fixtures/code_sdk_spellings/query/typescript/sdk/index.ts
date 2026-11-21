import { query } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows of one name. */
export const lookup = query(async function lookup(name: string): Promise<Row[]> {
  return [];
});
