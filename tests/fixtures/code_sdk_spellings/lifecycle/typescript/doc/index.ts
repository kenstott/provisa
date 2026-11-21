export interface Row {
  id: number;
  name: string;
}

/** Open the client.
 * @on-start
 */
export async function start(values: Record<string, string>): Promise<void> {}

/** Close the client.
 * @on-stop
 */
export function stop(): void {}

/** @health */
export async function status(): Promise<string> {
  return "ok";
}

/** @table */
export function rows(): Row[] {
  return [];
}
