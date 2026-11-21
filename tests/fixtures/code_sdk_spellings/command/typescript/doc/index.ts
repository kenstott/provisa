export interface Row {
  id: number;
  name: string;
}

/** Cancel one order.
 * @command
 */
export async function cancel(order_id: string): Promise<number> {
  return 1;
}
