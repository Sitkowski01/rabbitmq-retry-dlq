# RabbitMQ: kolejka, konsument, ponawianie i dead letter queue

Mały, działający przykład wzorca, który spina cztery rzeczy: **kolejkę, konsumenta,
ponawianie z narastającym opóźnieniem i obsługę komunikatów, których nie da się przetworzyć.**

RabbitMQ stoi w Dockerze, kod jest w TypeScripcie na `amqplib`. Całość uruchamia się
trzema poleceniami.

---

## Uruchomienie

```bash
npm install
npm run up          # RabbitMQ w Dockerze (panel: http://localhost:15672, guest/guest)
npm run consumer    # w jednym terminalu — nasłuchuje
npm run producer    # w drugim — publikuje pięć komunikatów
```

Sprzątanie: `npm run down`.

---

## Topologia

```
                        ┌──────────────────────┐
  publish ─────────────►│  orders  (direct)    │
                        └──────────┬───────────┘
                                   │ rk: order.created
                                   ▼
                        ┌──────────────────────┐
                        │  orders.process      │◄──── konsument (prefetch 1)
                        └──────────┬───────────┘
                                   │ błąd przejściowy
                                   ▼
                        ┌───────────────────────┐
                        │  orders.retry (direct) │
                        └──┬─────────┬──────────┬┘
                    rk:1   │   rk:2  │   rk:3   │
                           ▼         ▼          ▼
                     retry.5s    retry.15s   retry.60s
                     x-message-ttl + dead-letter z powrotem na `orders`
                           └─────────┴──────────┘
                                   │ po wygaśnięciu TTL
                                   ▼
                            znów orders.process

  błąd trwały albo wyczerpane próby ──► orders.dlx ──► orders.dlq (parking, bez konsumenta)
```

### Dlaczego opóźnienie trzyma broker, a nie konsument

Ponowienie można zrobić `setTimeout` w konsumencie, ale wtedy proces blokuje slot
`prefetch` i **wszystkie zaplanowane ponowienia giną przy restarcie**. Tutaj komunikat
ląduje w kolejce z `x-message-ttl`, z której nikt nie czyta — po wygaśnięciu TTL broker
sam odkłada go z powrotem na główny exchange. Opóźnienie jest trwałe i nie kosztuje
konsumenta ani jednego wątku.

### Dlaczego `ack`, a nie `nack(requeue)`

`nack` z `requeue: true` wraca na początek tej samej kolejki **natychmiast** — czyli
robi pętlę bez opóźnienia, która potrafi zapchać brokera w kilka sekund. Tutaj konsument
publikuje komunikat na kolejkę opóźniającą i dopiero wtedy potwierdza oryginał:
odpowiedzialność przechodzi na retry, a główna kolejka zwalnia się od razu.

### Rozróżnienie błędów

Nie każdy błąd zasługuje na trzy podejścia:

| Rodzaj | Przykład | Co się dzieje |
|---|---|---|
| `TransientError` | zewnętrzne API nie odpowiedziało | ponowienie: 5s → 15s → 60s, potem DLQ |
| `PermanentError` | zamówienie z ujemną kwotą | **prosto na DLQ**, bez marnowania prób |
| nieparsowalny JSON | uszkodzony komunikat | **prosto na DLQ** — nie zacznie się parsować |

---

## Co robi przykład

Producent wysyła pięć komunikatów pokazujących wszystkie ścieżki:

| Komunikat | Zachowanie | Oczekiwany wynik |
|---|---|---|
| `ORD-1001`, `ORD-1004` | poprawne | przetworzone za pierwszym razem |
| `ORD-1002` | pada dwa razy, za trzecim przechodzi | **dowód, że ponawianie działa** |
| `ORD-1003` | ujemna kwota, błąd trwały | **dowód, że DLQ działa** |
| uszkodzony JSON | nie da się sparsować | DLQ bez ponawiania |

### Rzeczywiste wyjście

```
22:29:39 👂 nasłuchuję na orders.process (Ctrl+C kończy)
22:29:41 ✅ zamówienie ORD-1001 rozliczone na 249.99 zł  [próba 1]
22:29:41 🔁 ORD-1002: zewnętrzna płatność nie odpowiedziała (próba 1 z 3) → ponowienie za 5s
22:29:41 ☠️  ORD-1003: ujemna kwota (-15 zł) — nie da się przetworzyć → DLQ (błąd trwały)
22:29:41 ✅ zamówienie ORD-1004 rozliczone na 1200 zł  [próba 1]
22:29:41 ☠️  komunikat nie jest poprawnym JSON-em → prosto na DLQ
22:29:46 🔁 ORD-1002: zewnętrzna płatność nie odpowiedziała (próba 2 z 3) → ponowienie za 15s
22:30:01 ✅ zamówienie ORD-1002 rozliczone po 3 podejściach  [próba 3]
```

Znaczniki czasu pokazują prawdziwe opóźnienia: **22:29:41 → 22:29:46 (5s) → 22:30:01 (15s)**.

Stan kolejek po przebiegu:

```
name              messages
orders.process    0
orders.retry.5s   0
orders.retry.15s  0
orders.retry.60s  0
orders.dlq        2     ← ORD-1003 i uszkodzony JSON
```

---

## Struktura

```
src/topology.ts   deklaracja exchange'y i kolejek — idempotentna, wołana przez obie strony
src/producer.ts   publikacja na confirm channel (broker potwierdza każdą wiadomość)
src/consumer.ts   przetwarzanie, decyzja retry / DLQ, nagłówek x-attempt
src/orders.ts     udawana logika biznesowa z trzema scenariuszami
docker-compose.yml
```

## Dwie rzeczy, które w prawdziwym systemie wyglądałyby inaczej

**Licznik prób siedzi w nagłówku `x-attempt`.** To wystarcza tutaj, ale przy komunikatach
wędrujących przez kilka usług lepiej trzymać stan próby w bazie razem z identyfikatorem
zamówienia — nagłówek da się zgubić przy przepakowaniu.

**Konsument nie jest idempotentny.** `ORD-1002` przechodzi za trzecim razem, bo licznik
awarii jest w pamięci procesu. W produkcji ponowienie musi być bezpieczne przy wielokrotnym
wykonaniu — inaczej trzykrotne podejście do płatności obciąży klienta trzy razy.
