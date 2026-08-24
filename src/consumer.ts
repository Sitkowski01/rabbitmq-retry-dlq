import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import { connectWithRetry, log, pilnujPolaczenia } from "./amqp.js";
import {
  assertTopology,
  EXCHANGE_DLX,
  EXCHANGE_RETRY,
  MAX_RETRIES,
  QUEUE_MAIN,
  RETRY_DELAYS_MS,
  ROUTING_KEY,
  retryRoutingKey,
} from "./topology.js";
import { parseOrder, PermanentError, processOrder } from "./orders.js";

async function main() {
  const conn = await connectWithRetry();
  pilnujPolaczenia(conn);

  // Confirm channel także po stronie konsumenta: komunikat przekazywany na retry
  // albo na DLQ musi być potwierdzony przez brokera ZANIM potwierdzimy oryginał.
  // Bez tego awaria brokera w oknie zapisu gubiła komunikat bezpowrotnie.
  const ch: ConfirmChannel = await conn.createConfirmChannel();
  ch.on("error", (e) => log("⚠️ ", `błąd kanału: ${e.message}`));

  await assertTopology(ch);
  await ch.prefetch(1);

  /** Publikuje i czeka na potwierdzenie brokera. */
  const publishConfirmed = (
    exchange: string,
    rk: string,
    msg: ConsumeMessage,
    headers: Record<string, unknown>,
  ) =>
    new Promise<void>((resolve, reject) => {
      ch.publish(
        exchange,
        rk,
        msg.content,
        {
          persistent: true,
          // messageId i contentType niosą identyfikator nadany przez producenta —
          // bez nich wpis na DLQ jest anonimowy i człowiek nie wie, czego dotyczy.
          messageId: msg.properties.messageId,
          contentType: msg.properties.contentType,
          headers,
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });

  const naDlq = async (msg: ConsumeMessage, powod: string, opis: string, proba: number) => {
    await publishConfirmed(EXCHANGE_DLX, ROUTING_KEY, msg, {
      ...msg.properties.headers,
      "x-attempt": proba,
      "x-death-reason": powod,
      "x-last-error": opis,
    });
    ch.ack(msg);
  };

  let wTrakcie = false;
  let zamykanie = false;
  let consumerTag = "";

  log("👂", `nasłuchuję na ${QUEUE_MAIN} · ponowienia: ${RETRY_DELAYS_MS.map((d) => d / 1000 + "s").join(" → ")} · Ctrl+C kończy`);

  const { consumerTag: tag } = await ch.consume(QUEUE_MAIN, async (msg) => {
    if (!msg) return;
    wTrakcie = true;
    try {
      // Nagłówek mógł przyjść z zewnątrz w dowolnej postaci. Bez tej ochrony
      // niepoprawna wartość dawała NaN, a publikacja szła na `retry.NaN` —
      // klucz bez powiązania, więc broker po cichu wyrzucał komunikat.
      const surowa = Number(msg.properties.headers?.["x-attempt"]);
      const proba = (Number.isFinite(surowa) && surowa >= 0 ? Math.floor(surowa) : 0) + 1;

      let order;
      try {
        order = parseOrder(JSON.parse(msg.content.toString()));
      } catch (e) {
        // Zły JSON albo zły kształt nigdy się nie naprawi — retry nie ma sensu.
        const opis = e instanceof Error ? e.message : String(e);
        log("☠️ ", `komunikat odrzucony przy walidacji (${opis}) → DLQ`);
        await naDlq(msg, "invalid-message", opis, proba);
        return;
      }

      try {
        log("✅", `${await processOrder(order)}  [podejście ${proba}]`);
        ch.ack(msg);
      } catch (err) {
        const opis = err instanceof Error ? err.message : String(err);

        if (err instanceof PermanentError) {
          log("☠️ ", `${order.id}: ${opis} → DLQ (błąd trwały)`);
          await naDlq(msg, "permanent-error", opis, proba);
          return;
        }
        if (proba > MAX_RETRIES) {
          log("☠️ ", `${order.id}: ${opis} → DLQ (wyczerpane ${MAX_RETRIES} ponowienia)`);
          await naDlq(msg, "max-retries-exceeded", opis, proba);
          return;
        }

        const delay = RETRY_DELAYS_MS[proba - 1];
        log("🔁", `${order.id}: ${opis} → ponowienie za ${delay / 1000}s  [${proba} z ${MAX_RETRIES}]`);
        await publishConfirmed(EXCHANGE_RETRY, retryRoutingKey(proba), msg, {
          ...msg.properties.headers,
          "x-attempt": proba,
          "x-last-error": opis,
        });
        // ack dopiero po potwierdzeniu — odpowiedzialność przejęła kolejka opóźniająca.
        // nack z requeue wróciłby tu natychmiast i zrobił pętlę bez opóźnienia.
        ch.ack(msg);
      }
    } catch (e) {
      log("⚠️ ", `nieoczekiwany błąd obsługi: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      wTrakcie = false;
      if (zamykanie) void domknij();
    }
  });
  consumerTag = tag;

  const domknij = async () => {
    try {
      await ch.close();
      await conn.close();
    } catch {
      /* kanał mógł już być zamknięty */
    }
    process.exit(0);
  };

  // Najpierw anulujemy subskrypcję, potem czekamy na komunikat w locie.
  // Wcześniejsze zamykanie kanału powodowało ack na zamkniętym kanale
  // i wywrotkę na nieobsłużonym odrzuceniu.
  process.on("SIGINT", async () => {
    if (zamykanie) return;
    zamykanie = true;
    log("👋", "kończę — anuluję subskrypcję i czekam na komunikat w locie");
    try {
      await ch.cancel(consumerTag);
    } catch {
      /* nic */
    }
    if (!wTrakcie) await domknij();
  });
}

main().catch((e) => {
  console.error("konsument padł:", e);
  process.exit(1);
});
