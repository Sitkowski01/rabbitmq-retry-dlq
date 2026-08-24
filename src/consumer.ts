import amqp from "amqplib";
import {
  assertTopology,
  EXCHANGE_DLX,
  EXCHANGE_RETRY,
  MAX_ATTEMPTS,
  QUEUE_MAIN,
  RETRY_DELAYS_MS,
  ROUTING_KEY,
  retryRoutingKey,
} from "./topology.js";
import { PermanentError, processOrder, type Order } from "./orders.js";

const URL = process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5672";

const ts = () => new Date().toISOString().slice(11, 19);
const log = (icon: string, msg: string) => console.log(`${ts()} ${icon} ${msg}`);

async function main() {
  const conn = await amqp.connect(URL);
  const ch = await conn.createChannel();
  await assertTopology(ch);

  // Bez tego jeden konsument zassałby całą kolejkę do pamięci i ponowienia
  // przestałyby cokolwiek znaczyć.
  await ch.prefetch(1);

  log("👂", `nasłuchuję na ${QUEUE_MAIN} (Ctrl+C kończy)`);

  await ch.consume(QUEUE_MAIN, async (msg) => {
    if (!msg) return;

    const attempt = Number(msg.properties.headers?.["x-attempt"] ?? 0) + 1;
    let order: Order;

    try {
      order = JSON.parse(msg.content.toString()) as Order;
    } catch {
      // Nieparsowalny komunikat nigdy nie zacznie się parsować — retry nie ma sensu.
      log("☠️ ", "komunikat nie jest poprawnym JSON-em → prosto na DLQ");
      ch.publish(EXCHANGE_DLX, ROUTING_KEY, msg.content, {
        persistent: true,
        headers: { ...msg.properties.headers, "x-death-reason": "malformed-json" },
      });
      ch.ack(msg);
      return;
    }

    try {
      const result = await processOrder(order);
      log("✅", `${result}  [próba ${attempt}]`);
      ch.ack(msg);
      return;
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      const permanent = err instanceof PermanentError;

      // Błąd trwały nie zmieni się od powtórzenia — nie marnujemy na niego trzech podejść.
      if (permanent || attempt >= MAX_ATTEMPTS) {
        const why = permanent
          ? "błąd trwały"
          : `wyczerpane ${MAX_ATTEMPTS} próby`;
        log("☠️ ", `${order.id}: ${reason} → DLQ (${why})`);
        ch.publish(EXCHANGE_DLX, ROUTING_KEY, msg.content, {
          persistent: true,
          headers: {
            ...msg.properties.headers,
            "x-attempt": attempt,
            "x-death-reason": permanent ? "permanent-error" : "max-attempts-exceeded",
            "x-last-error": reason,
          },
        });
        ch.ack(msg);
        return;
      }

      const delay = RETRY_DELAYS_MS[attempt - 1];
      log(
        "🔁",
        `${order.id}: ${reason} → ponowienie za ${delay / 1000}s  [próba ${attempt} z ${MAX_ATTEMPTS}]`,
      );
      ch.publish(EXCHANGE_RETRY, retryRoutingKey(attempt), msg.content, {
        persistent: true,
        headers: { ...msg.properties.headers, "x-attempt": attempt, "x-last-error": reason },
      });
      // ack, bo odpowiedzialność za komunikat przejmuje kolejka opóźniająca.
      // nack z requeue wróciłby tu natychmiast i zrobił pętlę bez opóźnienia.
      ch.ack(msg);
    }
  });

  process.on("SIGINT", async () => {
    log("👋", "zamykam połączenie");
    await ch.close();
    await conn.close();
    process.exit(0);
  });
}

main().catch((e) => {
  console.error("konsument padł:", e);
  process.exit(1);
});
