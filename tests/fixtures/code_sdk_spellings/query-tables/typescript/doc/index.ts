import type { TableRef } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}

export interface Match {
  left_id: number;
  right_id: number;
}

/** Rows of two tables that belong together.
 * @query
 */
export async function pairs(left: TableRef<Row>, right: TableRef<Row>): Promise<Match[]> {
  return [];
}
