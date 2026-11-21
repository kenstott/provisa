import { query, type TableRef } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export interface Match {
  left_id: number;
  right_id: number;
}

/** Rows of two tables that belong together. */
export const pairs = query(
  async function pairs(left: TableRef<Row>, right: TableRef<Row>): Promise<Match[]> {
    return [];
  },
);
