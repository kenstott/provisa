export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Each row as it arrives.
 * @stream
 */
export async function* ticks(inputs: Inputs): AsyncGenerator<Row> {}
