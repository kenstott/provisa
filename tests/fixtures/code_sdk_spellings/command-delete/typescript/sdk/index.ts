import { command } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** The delete of rows. */
export const deleteRows = command(
  async function deleteRows(keys: number[]): Promise<number> {
    return 0;
  },
  { delete: "rows" },
);
