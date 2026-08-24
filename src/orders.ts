/**
 * Udawana logika biznesowa. Trzy typy zamówień, żeby pokazać trzy ścieżki:
 *
 *   ok      — przechodzi za pierwszym razem
 *   flaky   — pada dwa razy, przy trzeciej próbie przechodzi  → dowód, że retry działa
 *   poison  — pada zawsze                                     → dowód, że DLQ działa
 */

export type OrderKind = "ok" | "flaky" | "poison";

export interface Order {
  id: string;
  kind: OrderKind;
  amount: number;
}

/** Ile razy dany identyfikator już padł — w prawdziwym systemie zrobiłby to stan w bazie. */
const failures = new Map<string, number>();

export class TransientError extends Error {}
export class PermanentError extends Error {}

export async function processOrder(order: Order): Promise<string> {
  switch (order.kind) {
    case "ok":
      return `zamówienie ${order.id} rozliczone na ${order.amount} zł`;

    case "flaky": {
      const seen = (failures.get(order.id) ?? 0) + 1;
      failures.set(order.id, seen);
      if (seen <= 2) {
        throw new TransientError(
          `zewnętrzna płatność nie odpowiedziała (próba ${seen} z 3)`,
        );
      }
      return `zamówienie ${order.id} rozliczone po ${seen} podejściach`;
    }

    case "poison":
      throw new PermanentError(
        `zamówienie ${order.id} ma ujemną kwotę (${order.amount} zł) — nie da się go przetworzyć`,
      );
  }
}
