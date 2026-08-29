import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  assertTopology,
  odczytajProgi,
  EXCHANGE,
  EXCHANGE_DLX,
  EXCHANGE_RETRY,
  MAX_RETRIES,
  QUEUE_DLQ,
  QUEUE_MAIN,
  RETRY_DELAYS_MS,
  ROUTING_KEY,
  retryQueueName,
  retryRoutingKey,
} from "./topology.js";

/** Kanał-atrapa: zapisuje, co zostało zadeklarowane, zamiast gadać z brokerem. */
function kanalAtrapa() {
  const exchanges: { nazwa: string; typ: string; opcje: unknown }[] = [];
  const queues: { nazwa: string; opcje: Record<string, unknown> }[] = [];
  const bindings: { kolejka: string; exchange: string; rk: string }[] = [];

  const ch = {
    assertExchange: async (nazwa: string, typ: string, opcje: unknown) => {
      exchanges.push({ nazwa, typ, opcje });
    },
    assertQueue: async (nazwa: string, opcje: Record<string, unknown> = {}) => {
      queues.push({ nazwa, opcje });
    },
    bindQueue: async (kolejka: string, exchange: string, rk: string) => {
      bindings.push({ kolejka, exchange, rk });
    },
  };

  return { ch, exchanges, queues, bindings };
}

describe("odczytajProgi", () => {
  it("brak zmiennej środowiskowej daje progi domyślne", () => {
    assert.deepEqual(odczytajProgi(undefined), [5000, 15000, 60000]);
  });

  it("czyta własne progi", () => {
    assert.deepEqual(odczytajProgi("100, 200, 300"), [100, 200, 300]);
  });

  it("pomija pojedyncze śmieci, zostawiając poprawne wartości", () => {
    assert.deepEqual(odczytajProgi("100, abc, 300"), [100, 300]);
  });

  it("wywraca się, gdy nie zostanie ani jedna poprawna liczba", () => {
    // Sedno poprawki: wczesniej pusta lista dawala MAX_RETRIES = 0, wiec kazdy
    // blad przejsciowy szedl OD RAZU na DLQ — bez wyjatku i bez sladu w logach.
    for (const zle of ["abc", "", "0", "-5", "   "]) {
      assert.throws(
        () => odczytajProgi(zle),
        /nie zawiera ani jednej dodatniej liczby/,
        `powinno odrzucic: "${zle}"`,
      );
    }
  });
});

describe("progi ponawiania", () => {
  it("domyślnie trzy progi: 5s, 15s, 60s", () => {
    // Test czyta wartosc domyslna — w CI zmienna RETRY_DELAYS nie jest ustawiana.
    assert.deepEqual(RETRY_DELAYS_MS, [5000, 15000, 60000]);
  });

  it("MAX_RETRIES to liczba progów", () => {
    assert.equal(MAX_RETRIES, RETRY_DELAYS_MS.length);
  });

  it("progi rosną — inaczej narastające opóźnienie nie narasta", () => {
    for (let i = 1; i < RETRY_DELAYS_MS.length; i++) {
      assert.ok(
        RETRY_DELAYS_MS[i] > RETRY_DELAYS_MS[i - 1],
        `próg ${i} nie jest większy od poprzedniego`,
      );
    }
  });

  it("nazwa kolejki opóźniającej niesie próg w sekundach", () => {
    assert.equal(retryQueueName(5000), "orders.retry.5s");
    assert.equal(retryQueueName(60000), "orders.retry.60s");
  });

  it("klucz routingu numeruje podejście od jedynki", () => {
    assert.equal(retryRoutingKey(1), "retry.1");
    assert.equal(retryRoutingKey(3), "retry.3");
  });
});

describe("assertTopology", () => {
  it("deklaruje trzy wymiany, wszystkie direct i trwałe", async () => {
    const { ch, exchanges } = kanalAtrapa();
    await assertTopology(ch as never);

    const nazwy = exchanges.map((e) => e.nazwa);
    assert.deepEqual(nazwy.sort(), [EXCHANGE, EXCHANGE_DLX, EXCHANGE_RETRY].sort());
    assert.ok(exchanges.every((e) => e.typ === "direct"));
    assert.ok(exchanges.every((e) => (e.opcje as { durable: boolean }).durable));
  });

  it("kolejka główna jest podpięta pod wymianę orders", async () => {
    const { ch, bindings } = kanalAtrapa();
    await assertTopology(ch as never);

    assert.ok(
      bindings.some(
        (b) => b.kolejka === QUEUE_MAIN && b.exchange === EXCHANGE && b.rk === ROUTING_KEY,
      ),
    );
  });

  it("DLQ wisi na wymianie dead letter i nie ma TTL", async () => {
    const { ch, queues, bindings } = kanalAtrapa();
    await assertTopology(ch as never);

    const dlq = queues.find((q) => q.nazwa === QUEUE_DLQ);
    assert.ok(dlq, "brak kolejki DLQ");
    // Parking: komunikat ma tu czekac na czlowieka, wiec zaden TTL nie moze go sprzatnac.
    assert.equal(dlq!.opcje["messageTtl"], undefined);
    assert.ok(
      bindings.some((b) => b.kolejka === QUEUE_DLQ && b.exchange === EXCHANGE_DLX),
    );
  });

  it("każdy próg dostaje własną kolejkę z TTL i powrotem na orders", async () => {
    const { ch, queues, bindings } = kanalAtrapa();
    await assertTopology(ch as never);

    for (const [i, delay] of RETRY_DELAYS_MS.entries()) {
      const nazwa = retryQueueName(delay);
      const kolejka = queues.find((q) => q.nazwa === nazwa);

      assert.ok(kolejka, `brak kolejki ${nazwa}`);
      assert.equal(kolejka!.opcje["messageTtl"], delay);
      // To jest cala sztuczka: opoznienie trzyma broker, a po wygasnieciu TTL
      // sam odklada komunikat na glowna wymiane.
      assert.equal(kolejka!.opcje["deadLetterExchange"], EXCHANGE);
      assert.equal(kolejka!.opcje["deadLetterRoutingKey"], ROUTING_KEY);

      assert.ok(
        bindings.some(
          (b) =>
            b.kolejka === nazwa &&
            b.exchange === EXCHANGE_RETRY &&
            b.rk === retryRoutingKey(i + 1),
        ),
        `kolejka ${nazwa} nie jest podpięta pod ${retryRoutingKey(i + 1)}`,
      );
    }
  });

  it("żadna kolejka opóźniająca nie odsyła sama do siebie", async () => {
    const { ch, queues } = kanalAtrapa();
    await assertTopology(ch as never);

    // Petla nieskonczona: kolejka z TTL wskazujaca na wlasna wymiane
    // odbijalaby komunikat w kolko, bez zadnego konsumenta.
    for (const q of queues) {
      assert.notEqual(q.opcje["deadLetterExchange"], EXCHANGE_RETRY);
    }
  });

  it("jest idempotentna — dwa wywołania dają ten sam zestaw", async () => {
    const raz = kanalAtrapa();
    await assertTopology(raz.ch as never);

    const dwa = kanalAtrapa();
    await assertTopology(dwa.ch as never);
    await assertTopology(dwa.ch as never);

    assert.deepEqual(
      [...new Set(dwa.queues.map((q) => q.nazwa))].sort(),
      raz.queues.map((q) => q.nazwa).sort(),
    );
  });
});
