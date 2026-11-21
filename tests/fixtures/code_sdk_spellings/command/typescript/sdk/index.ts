import { command } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Cancel one order. */
export const cancel = command(async function cancel(order_id: string): Promise<number> {
  return 1;
});
