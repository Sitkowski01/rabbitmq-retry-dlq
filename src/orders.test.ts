import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  parseOrder,
  processOrder,
  PermanentError,
  TransientError,
  RODZAJE,
} from "./orders.js";

describe("parseOrder", () => {
  it("przepuszcza poprawne zamówienie", () => {
    const o = parseOrder({ id: "A-1", kind: "ok", amount: 99.5 });
    assert.deepEqual(o, { id: "A-1", kind: "ok", amount: 99.5 });
  });

  it("przyjmuje każdy zadeklarowany rodzaj", () => {
    for (const kind of RODZAJE) {
      assert.equal(parseOrder({ id: "X", kind, amount: 1 }).kind, kind);
    }
  });

  it("obcina nadmiarowe pola zamiast je przepuszczać", () => {
    const o = parseOrder({ id: "A-1", kind: "ok", amount: 1, admin: true } as unknown);
    assert.deepEqual(Object.keys(o).sort(), ["amount", "id", "kind"]);
  });

  // Kazdy z ponizszych przypadkow to blad TRWALY: ponawianie go nie naprawi,
  // wiec komunikat ma isc prosto na DLQ, a nie krecic sie przez trzy proby.
  const zle: [string, unknown][] = [
    ["null", null],
    ["liczba zamiast obiektu", 42],
    ["napis zamiast obiektu", '{"id":"A"}'],
    ["brak id", { kind: "ok", amount: 1 }],
    ["puste id", { id: "", kind: "ok", amount: 1 }],
    ["id nie jest napisem", { id: 7, kind: "ok", amount: 1 }],
    ["nieznany rodzaj", { id: "A", kind: "refund", amount: 1 }],
    ["brak rodzaju", { id: "A", amount: 1 }],
    ["amount jako napis", { id: "A", kind: "ok", amount: "100" }],
    ["amount NaN", { id: "A", kind: "ok", amount: Number.NaN }],
    ["amount Infinity", { id: "A", kind: "ok", amount: Number.POSITIVE_INFINITY }],
  ];

  for (const [opis, wejscie] of zle) {
    it(`odrzuca błędem trwałym: ${opis}`, () => {
      assert.throws(() => parseOrder(wejscie), PermanentError);
    });
  }
});

describe("processOrder", () => {
  it("ok przechodzi za pierwszym razem", async () => {
    const wynik = await processOrder({ id: "OK-1", kind: "ok", amount: 10 });
    assert.match(wynik, /rozliczone/);
  });

  it("flaky pada dwa razy, za trzecim przechodzi", async () => {
    const order = { id: `FLAKY-${Date.now()}`, kind: "flaky" as const, amount: 10 };

    await assert.rejects(() => processOrder(order), TransientError);
    await assert.rejects(() => processOrder(order), TransientError);

    const wynik = await processOrder(order);
    assert.match(wynik, /po 3 podejściach/);
  });

  it("flaky liczy podejścia osobno dla każdego identyfikatora", async () => {
    const a = { id: `A-${Date.now()}`, kind: "flaky" as const, amount: 1 };
    const b = { id: `B-${Date.now()}`, kind: "flaky" as const, amount: 1 };

    await assert.rejects(() => processOrder(a), TransientError);
    // b jest widziane pierwszy raz, wiec tez ma paść — gdyby licznik był wspólny,
    // b przeszłoby po jednej porażce a.
    await assert.rejects(() => processOrder(b), TransientError);
  });

  it("stubborn zawsze pada błędem przejściowym", async () => {
    const order = { id: "STUB-1", kind: "stubborn" as const, amount: 10 };
    for (let i = 0; i < 4; i++) {
      await assert.rejects(() => processOrder(order), TransientError);
    }
  });

  it("poison pada błędem trwałym — bez marnowania ponowień", async () => {
    await assert.rejects(
      () => processOrder({ id: "POISON-1", kind: "poison", amount: -5 }),
      PermanentError,
    );
  });

  it("błąd trwały nie jest przejściowym i odwrotnie", () => {
    // Konsument rozroznia je przez instanceof — gdyby jeden dziedziczyl po drugim,
    // poison trafialby w galaz retry.
    assert.ok(!(new PermanentError("x") instanceof TransientError));
    assert.ok(!(new TransientError("x") instanceof PermanentError));
  });
});
