# RabbitMQ: kolejka, konsument, ponawianie i dead letter queue

[![CI](https://github.com/Sitkowski01/rabbitmq-retry-dlq/actions/workflows/ci.yml/badge.svg)](https://github.com/Sitkowski01/rabbitmq-retry-dlq/actions/workflows/ci.yml)

Mały, działający przykład wzorca, który spina cztery rzeczy: **kolejkę, konsumenta,
ponawianie z narastającym opóźnieniem i obsługę komunikatów, których nie da się przetworzyć.**

RabbitMQ stoi w Dockerze, kod jest w TypeScripcie na `amqplib`.

---

## Uruchomienie

```bash
npm install
npm run up          # RabbitMQ w Dockerze (panel: http://localhost:15672, guest/guest)
npm run consumer    # w jednym terminalu — nasłuchuje
npm run producer    # w drugim — publikuje siedem komunikatów
```

Sprzątanie: `npm run down`.

Domyślne opóźnienia to 5s / 15s / 60s. Do szybkiego przebiegu:

```bash
RETRY_DELAYS="2000,4000,6000" npm run consumer
RETRY_DELAYS="2000,4000,6000" npm run producer
```

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

  błąd trwały, zły kształt albo wyczerpane ponowienia ──► orders.dlx ──► orders.dlq
```

**Trzy ponowienia, czyli w sumie cztery podejścia.** Pierwsze wykonanie nie jest
ponowieniem: komunikat, który pada za każdym razem, przechodzi przez 5s, 15s i 60s,
a dopiero czwarta nieudana próba wysyła go na DLQ.

### Dlaczego opóźnienie trzyma broker, a nie konsument

Ponowienie można zrobić `setTimeout` w konsumencie, ale wtedy proces blokuje slot
`prefetch` i **wszystkie zaplanowane ponowienia giną przy restarcie**. Tutaj komunikat
ląduje w kolejce z `x-message-ttl`, z której nikt nie czyta — po wygaśnięciu TTL broker
sam odkłada go z powrotem na główny exchange.

### Dlaczego `ack`, a nie `nack(requeue)`

`nack` z `requeue: true` wraca na początek tej samej kolejki **natychmiast** — czyli
robi pętlę bez opóźnienia, która potrafi zapchać brokera w kilka sekund. Tutaj konsument
publikuje komunikat na kolejkę opóźniającą, **czeka na potwierdzenie brokera** i dopiero
wtedy potwierdza oryginał. Kolejność ma znaczenie: odwrotna gubi komunikat, jeśli broker
padnie w oknie między jednym a drugim.

### Rozróżnienie błędów

Nie każdy błąd zasługuje na trzy podejścia:

| Rodzaj | Przykład | Co się dzieje |
|---|---|---|
| `TransientError` | zewnętrzne API nie odpowiedziało | ponowienie 5s → 15s → 60s, potem DLQ |
| `PermanentError` | zamówienie z ujemną kwotą | **prosto na DLQ**, bez marnowania prób |
| zły kształt komunikatu | uszkodzony JSON, nieznany `kind` | **prosto na DLQ** — to się nie naprawi |

---

## Co robi przykład

| Komunikat | Zachowanie | Oczekiwany wynik |
|---|---|---|
| `ORD-1001`, `ORD-1004` | poprawne | przetworzone za pierwszym razem |
| `ORD-1002` | pada dwa razy, za trzecim przechodzi | **ponawianie działa** |
| `ORD-1005` | pada zawsze błędem przejściowym | **wyczerpuje wszystkie trzy progi i ląduje na DLQ** |
| `ORD-1003` | ujemna kwota, błąd trwały | DLQ od razu |
| uszkodzony JSON | nie da się sparsować | DLQ bez ponawiania |
| `ORD-9999` | poprawny JSON, nieznany `kind` | DLQ bez ponawiania |

### Rzeczywisty przebieg

Z `RETRY_DELAYS="2000,4000,6000"`, na świeżym brokerze:

```
23:18:50 👂 nasłuchuję na orders.process · ponowienia: 2s → 4s → 6s · Ctrl+C kończy
23:18:55 ✅ zamówienie ORD-1001 rozliczone na 249.99 zł  [podejście 1]
23:18:55 🔁 ORD-1002: zewnętrzna płatność nie odpowiedziała → ponowienie za 2s  [1 z 3]
23:18:55 ☠️  ORD-1003: ujemna kwota (-15 zł) → DLQ (błąd trwały)
23:18:55 ✅ zamówienie ORD-1004 rozliczone na 1200 zł  [podejście 1]
23:18:55 🔁 ORD-1005: usługa rozliczeń niedostępna → ponowienie za 2s  [1 z 3]
23:18:55 ☠️  komunikat odrzucony przy walidacji (uszkodzony JSON) → DLQ
23:18:55 ☠️  komunikat odrzucony przy walidacji (nieznany rodzaj: nieznany) → DLQ
23:18:57 🔁 ORD-1002: → ponowienie za 4s  [2 z 3]
23:18:57 🔁 ORD-1005: → ponowienie za 4s  [2 z 3]
23:19:01 ✅ zamówienie ORD-1002 rozliczone po 3 podejściach  [podejście 3]
23:19:01 🔁 ORD-1005: → ponowienie za 6s  [3 z 3]
23:19:07 ☠️  ORD-1005: → DLQ (wyczerpane 3 ponowienia)
```

Znaczniki czasu pokazują prawdziwe opóźnienia: **23:18:55 → 23:18:57 (2s) → 23:19:01 (4s)
→ 23:19:07 (6s)**. Stan kolejek po przebiegu:

```
orders.process    0
orders.retry.2s   0
orders.retry.4s   0
orders.retry.6s   0
orders.dlq        4     ← ORD-1003, ORD-1005, uszkodzony JSON, ORD-9999
```

---

## Struktura

```
src/topology.ts   deklaracja exchange'y i kolejek — idempotentna, wołana przez obie strony
src/amqp.ts       połączenie z ponawianiem i nasłuchy na błąd/zamknięcie
src/producer.ts   publikacja na confirm channel (broker potwierdza każdą wiadomość)
src/consumer.ts   przetwarzanie, decyzja retry / DLQ, nagłówek x-attempt
src/orders.ts     walidacja kształtu komunikatu + udawana logika biznesowa
docker-compose.yml
```

## Co poprawiłem po przeglądzie kodu

Pierwszą wersję, która przechodziła testy, przepuściłem przez przegląd kodu
(Claude Code). Wyszło osiem rzeczy, dwie poważne:

- **Trzeci próg ponawiania był martwy.** Warunek `attempt >= MAX_ATTEMPTS` odsyłał
  komunikat na DLQ po dwóch ponowieniach, więc kolejka `orders.retry.60s` nigdy nie
  dostawała ruchu, a README obiecywało trzy. Teraz warunek brzmi `proba > MAX_RETRIES`,
  a `ORD-1005` w logu wyżej faktycznie przechodzi przez wszystkie trzy progi.
- **Komunikat z nieznanym `kind` ginął po cichu.** `JSON.parse(...) as Order` przepuszczał
  dowolny obiekt, `switch` nie miał gałęzi domyślnej, więc wynik był `undefined`,
  logowany jako sukces i potwierdzany. Teraz jest `parseOrder` z walidacją i `never`
  w gałęzi domyślnej, żeby kompilator pilnował kompletności.

Pozostałe sześć:

- **Przekazanie na kolejkę opóźniającą idzie przez confirm channel.** Wcześniej szło
  zwykłym kanałem — awaria brokera w tym oknie gubiła komunikat bez śladu.
- **SIGINT anuluje subskrypcję i czeka na komunikat w locie**, zamiast zamykać kanał
  pod trwającym przetwarzaniem.
- **Doszły nasłuchy `error` i `close` na połączeniu.** Restart brokera ubijał proces,
  nie zostawiając niczego w logu.
- **`NaN` w nagłówku `x-attempt` nie kieruje już na nieistniejący klucz `retry.NaN`.**
- **Połączenie ponawia się przy starcie.** Broker w Dockerze wstaje wolniej, niż
  `docker compose up` wraca do promptu.
- **Przekazywany komunikat zachowuje `messageId` i `contentType`**, więc wpis na DLQ
  da się powiązać z konkretnym zamówieniem.

## Dwie rzeczy, które w prawdziwym systemie wyglądałyby inaczej

**Licznik prób siedzi w nagłówku `x-attempt`.** Wystarcza tutaj, ale przy komunikatach
wędrujących przez kilka usług lepiej trzymać stan próby w bazie razem z identyfikatorem
zamówienia — nagłówek da się zgubić przy przepakowaniu.

**Konsument nie jest idempotentny.** `ORD-1002` przechodzi za trzecim razem, bo licznik
awarii jest w pamięci procesu. W produkcji ponowienie musi być bezpieczne przy wielokrotnym
wykonaniu — inaczej trzykrotne podejście do płatności obciąży klienta trzy razy.

## Testy

35 testów na wbudowanym runnerze Node — bez brokera, bez dodatkowych zależności:

```bash
npm test
```

- **walidacja komunikatu** — jedenaście kształtów, które muszą polecieć błędem trwałym
  prosto na DLQ, zamiast kręcić się przez trzy ponowienia
- **logika ponowień** — że `flaky` przechodzi za trzecim podejściem, `stubborn` nigdy,
  a licznik podejść jest osobny dla każdego identyfikatora
- **topologia** — na kanale-atrapie: że każda kolejka opóźniająca ma TTL równy swojemu
  progowi i odsyła na główną wymianę, że DLQ nie ma TTL (parking, nie przelotka)
  i że żadna kolejka nie odsyła sama do siebie, co dałoby pętlę bez konsumenta
- **idempotentność** `assertTopology` — dwa wywołania dają ten sam zestaw
- **konfiguracja progów** — błędna wartość `RETRY_DELAYS` przerywa start z czytelnym
  komunikatem, zamiast po cichu zostawiać zero ponowień
