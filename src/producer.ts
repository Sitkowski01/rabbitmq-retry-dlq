import amqp from "amqplib";
import { assertTopology, EXCHANGE, ROUTING_KEY } from "./topology.js";
import type { Order } from "./orders.js";

const URL = process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5672";

const ORDERS: Order[] = [
  { id: "ORD-1001", kind: "ok", amount: 249.99 },
  { id: "ORD-1002", kind: "flaky", amount: 89.5 },
  { id: "ORD-1003", kind: "poison", amount: -15 },
  { id: "ORD-1004", kind: "ok", amount: 1200 },
];

async function main() {
  const conn = await amqp.connect(URL);
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

  // Dodatkowo jeden komunikat, którego nie da się sparsować — sprawdza ścieżkę
  // "malformed" w konsumencie, która idzie prosto na DLQ, bez ponawiania.
  ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from("{to nie jest json"), {
    persistent: true,
    contentType: "application/json",
  });
  console.log("→ wysłane USZKODZONY  (nieparsowalny JSON)");

  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
  console.log("\nwszystkie publikacje potwierdzone przez brokera");
}

main().catch((e) => {
  console.error("producent padł:", e);
  process.exit(1);
});
