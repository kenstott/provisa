import { command } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** The insert of rows. */
export const insertRows = command(
  async function insertRows(rows: Row[]): Promise<number> {
    return 0;
  },
  { insert: "rows" },
);
