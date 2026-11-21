export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region.
 * @table
 */
export function rows(inputs: Inputs): Iterable<Row> {
  return [];
}
