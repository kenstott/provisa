import { command } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export interface Changes {
  name?: string;
}

/** The update of rows. */
export const updateRows = command(
  async function updateRows(keys: number[], changes: Changes): Promise<number> {
    return 0;
  },
  { update: "rows" },
);
