import type { Channel } from "amqplib";

/**
 * Cała topologia w jednym miejscu — deklarowana idempotentnie przez producenta
 * i przez konsumenta, żeby żadna ze stron nie zakładała, że ktoś inny ją zbudował.
 *
 *                        ┌──────────────────────┐
 *   publish ────────────►│  orders  (direct)    │
 *                        └──────────┬───────────┘
 *                                   │ rk: order.created
 *                                   ▼
 *                        ┌──────────────────────┐
 *                        │  orders.process      │◄──── konsument
 *                        └──────────┬───────────┘
 *                                   │ błąd przejściowy
 *                                   ▼
 *                        ┌───────────────────────┐
 *                        │  orders.retry (direct) │
 *                        └──┬─────────┬──────────┬┘
 *                    rk:1   │   rk:2  │   rk:3   │
 *                           ▼         ▼          ▼
 *                     retry.5s    retry.15s   retry.60s
 *                     x-message-ttl + dead-letter z powrotem na `orders`
 *                           └─────────┴──────────┘
 *                                   │ po wygaśnięciu TTL
 *                                   ▼
 *                            znów orders.process
 *
 *   po wyczerpaniu ponowień ──► orders.dlx ──► orders.dlq (parking, bez konsumenta)
 *
 * Dlaczego TTL, a nie `sleep` w konsumencie: opóźnienie trzyma broker, więc konsument
 * nie blokuje wątku ani prefetch slota, a ponowienia przeżywają restart procesu.
 */

export const EXCHANGE = "orders";
export const EXCHANGE_RETRY = "orders.retry";
export const EXCHANGE_DLX = "orders.dlx";

export const QUEUE_MAIN = "orders.process";
export const QUEUE_DLQ = "orders.dlq";

export const ROUTING_KEY = "order.created";

/**
 * Kolejne progi ponawiania. Indeks = numer nieudanej próby - 1.
 * Nadpisywalne przez RETRY_DELAYS (ms, po przecinku) — testy nie muszą czekać minuty.
 */
const DOMYSLNE_PROGI = [5000, 15000, 60000];

/**
 * Czyta progi ponawiania ze zmiennej środowiskowej.
 *
 * Pusty wynik jest traktowany jako błąd konfiguracji, a nie jako „zero ponowień".
 * Bez tego literówka w `RETRY_DELAYS` dawała pustą listę, `MAX_RETRIES` schodziło
 * do zera i **każdy błąd przejściowy leciał od razu na DLQ** — po cichu, bo nic
 * się nie wywracało i logi wyglądały normalnie.
 */
export function odczytajProgi(surowe: string | undefined): number[] {
  if (surowe === undefined) return DOMYSLNE_PROGI;

  const progi = surowe
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);

  if (progi.length === 0) {
    throw new Error(
      `RETRY_DELAYS="${surowe}" nie zawiera ani jednej dodatniej liczby milisekund. ` +
        `Popraw wartość albo usuń zmienną, żeby użyć domyślnych progów ` +
        `(${DOMYSLNE_PROGI.join(", ")}).`,
    );
  }

  return progi;
}

export const RETRY_DELAYS_MS: number[] = odczytajProgi(process.env.RETRY_DELAYS);

/**
 * Ile razy PONAWIAMY. Pierwsze wykonanie nie jest ponowieniem, więc komunikat
 * dostaje w sumie MAX_RETRIES + 1 podejść, zanim trafi na DLQ.
 */
export const MAX_RETRIES = RETRY_DELAYS_MS.length;

export const retryQueueName = (delayMs: number) => `orders.retry.${delayMs / 1000}s`;
export const retryRoutingKey = (attempt: number) => `retry.${attempt}`;

export async function assertTopology(ch: Channel): Promise<void> {
  await ch.assertExchange(EXCHANGE, "direct", { durable: true });
  await ch.assertExchange(EXCHANGE_RETRY, "direct", { durable: true });
  await ch.assertExchange(EXCHANGE_DLX, "direct", { durable: true });

  await ch.assertQueue(QUEUE_MAIN, { durable: true });
  await ch.bindQueue(QUEUE_MAIN, EXCHANGE, ROUTING_KEY);

  // Kolejka parkingowa — bez konsumenta. To tu lądują komunikaty,
  // których nie da się przetworzyć, żeby człowiek mógł je obejrzeć.
  await ch.assertQueue(QUEUE_DLQ, { durable: true });
  await ch.bindQueue(QUEUE_DLQ, EXCHANGE_DLX, ROUTING_KEY);

  // Kolejki opóźniające. Nikt z nich nie czyta — komunikat siedzi do wygaśnięcia TTL,
  // a potem broker sam odkłada go z powrotem na `orders`.
  for (const [i, delay] of RETRY_DELAYS_MS.entries()) {
    const name = retryQueueName(delay);
    await ch.assertQueue(name, {
      durable: true,
      messageTtl: delay,
      deadLetterExchange: EXCHANGE,
      deadLetterRoutingKey: ROUTING_KEY,
    });
    await ch.bindQueue(name, EXCHANGE_RETRY, retryRoutingKey(i + 1));
  }
}
