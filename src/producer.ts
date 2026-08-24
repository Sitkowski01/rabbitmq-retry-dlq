import { connectWithRetry, pilnujPolaczenia } from "./amqp.js";
import { assertTopology, EXCHANGE, ROUTING_KEY } from "./topology.js";
import type { Order } from "./orders.js";

const ORDERS: Order[] = [
  { id: "ORD-1001", kind: "ok", amount: 249.99 },
  { id: "ORD-1002", kind: "flaky", amount: 89.5 },
  { id: "ORD-1003", kind: "poison", amount: -15 },
  { id: "ORD-1004", kind: "ok", amount: 1200 },
  { id: "ORD-1005", kind: "stubborn", amount: 640 },
];

async function main() {
  const conn = await connectWithRetry();
  pilnujPolaczenia(conn);
  const ch = await conn.createConfirmChannel();
  await assertTopology(ch);

  for (const order of ORDERS) {
    // persistent + confirm channel: publikacja jest potwierdzona przez brokera,
    // więc wiadomo, że komunikat naprawdę wylądował w kolejce, a nie zniknął po drodze.
    ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(order)), {
      persistent: true,
      contentType: "application/json",
      messageId: order.id,
      headers: { "x-attempt": 0 },
    });
    console.log(`→ wysłane ${order.id.padEnd(9)} (${order.kind})`);
  }

  // Dwa komunikaty sprawdzające ścieżkę walidacji — oba idą prosto na DLQ,
  // bez marnowania ponowień na coś, co nigdy się nie naprawi.
  ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from("{to nie jest json"), {
    persistent: true,
    contentType: "application/json",
    messageId: "USZKODZONY-JSON",
  });
  console.log("→ wysłane USZKODZONY (nieparsowalny JSON)");

  ch.publish(
    EXCHANGE,
    ROUTING_KEY,
    Buffer.from(JSON.stringify({ id: "ORD-9999", kind: "nieznany", amount: 10 })),
    { persistent: true, contentType: "application/json", messageId: "ORD-9999" },
  );
  console.log("→ wysłane ORD-9999  (poprawny JSON, nieznany rodzaj)");

  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
  console.log("\nwszystkie publikacje potwierdzone przez brokera");
}

main().catch((e) => {
  console.error("producent padł:", e);
  process.exit(1);
});
