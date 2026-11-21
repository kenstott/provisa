import { table, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows of the keys, 100 at a time. */
export const rows = table(
  async function rows(context: ReadContext): Promise<Row[]> {
    return [];
  },
  { loader: 100 },
);
