/**
 * Udawana logika biznesowa. Cztery typy zamówień, żeby pokazać cztery ścieżki:
 *
 *   ok       — przechodzi za pierwszym razem
 *   flaky    — pada dwa razy, przy trzeciej próbie przechodzi → dowód, że retry działa
 *   stubborn — pada zawsze błędem przejściowym → wyczerpuje ponowienia i ląduje na DLQ
 *   poison   — pada błędem trwałym → DLQ od razu, bez marnowania prób
 */

export const RODZAJE = ["ok", "flaky", "stubborn", "poison"] as const;
export type OrderKind = (typeof RODZAJE)[number];

export interface Order {
  id: string;
  kind: OrderKind;
  amount: number;
}

export class TransientError extends Error {}
export class PermanentError extends Error {}

/**
 * Walidacja kształtu komunikatu. Bez tego `JSON.parse(...) as Order` przepuszcza
 * dowolny obiekt — a komunikat z nieznanym `kind` wpadał w `switch` bez gałęzi
 * domyślnej, zwracał `undefined` i był logowany jako sukces oraz potwierdzany.
 * Czyli po cichu ginął, zamiast trafić na DLQ.
 */
export function parseOrder(raw: unknown): Order {
  if (typeof raw !== "object" || raw === null)
    throw new PermanentError("komunikat nie jest obiektem");

  const o = raw as Record<string, unknown>;

  if (typeof o.id !== "string" || o.id.length === 0)
    throw new PermanentError("brak pola id");
  if (typeof o.kind !== "string" || !RODZAJE.includes(o.kind as OrderKind))
    throw new PermanentError(`nieznany rodzaj zamówienia: ${String(o.kind)}`);
  if (typeof o.amount !== "number" || !Number.isFinite(o.amount))
    throw new PermanentError("pole amount nie jest liczbą");

  return { id: o.id, kind: o.kind as OrderKind, amount: o.amount };
}

/** Ile razy dany identyfikator już padł — w prawdziwym systemie zrobiłby to stan w bazie. */
const failures = new Map<string, number>();

export async function processOrder(order: Order): Promise<string> {
  switch (order.kind) {
    case "ok":
      return `zamówienie ${order.id} rozliczone na ${order.amount} zł`;

    case "flaky": {
      const seen = (failures.get(order.id) ?? 0) + 1;
      failures.set(order.id, seen);
      if (seen <= 2) {
        throw new TransientError(
          `zewnętrzna płatność nie odpowiedziała (podejście ${seen})`,
        );
      }
      return `zamówienie ${order.id} rozliczone po ${seen} podejściach`;
    }

    case "stubborn":
      throw new TransientError(
        `usługa rozliczeń niedostępna dla ${order.id} — błąd przejściowy, ale trwały w skutkach`,
      );

    case "poison":
      throw new PermanentError(
        `zamówienie ${order.id} ma ujemną kwotę (${order.amount} zł) — nie da się go przetworzyć`,
      );

    default: {
      // Gałąź nieosiągalna po walidacji, ale kompilator pilnuje kompletności switcha:
      // dopisanie nowego rodzaju bez obsługi tutaj nie skompiluje się.
      const _wyczerpane: never = order.kind;
      throw new PermanentError(`nieobsłużony rodzaj: ${String(_wyczerpane)}`);
    }
  }
}
