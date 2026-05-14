# CafeRPC — Simulatore di Caffetteria con gRPC Bidirectional Streaming

## Descrizione dell'esercizio

L'esercizio richiede di implementare un sistema client/server che simuli la gestione degli ordini in una caffetteria, sfruttando il pattern di comunicazione **bidirectional streaming** di gRPC.

Il client invia un flusso continuo di richieste di prodotti (articolo + quantità), e il server risponde con un flusso continuo di conferme, aggiornando il totale parziale dell'ordine ad ogni articolo aggiunto. Quando il client segnala la fine dell'ordine inviando l'item `"fine"`, il server chiude lo stream e invia il riepilogo finale.

L'obiettivo è quindi:
- Definire il contratto di comunicazione in un file `.proto` con uno streaming bidirezionale
- Implementare il server che mantiene lo stato del totale durante tutta la sessione di streaming
- Implementare il client con un generatore Python che produce le richieste in sequenza
- Gestire casi di errore: articoli non presenti nel listino, quantità non valide, chiusura anticipata

---

## Architettura e workflow

```
client.py                                         server.py
    │                                                  │
    │  ──── stream di OrderRequest ────────────────▶   │
    │       { customer, item, quantity }               │
    │                                                  │  per ogni request:
    │                                                  │  - verifica item nel listino
    │                                                  │  - calcola subtotale
    │                                                  │  - aggiorna totale
    │                                                  │
    │  ◀──── stream di OrderResponse ─────────────────  │
    │       { status, message, partial_total }         │
    │                                                  │
    │  se item == "fine":                              │  yield SUMMARY + return
    │  se item non valido o qty ≤ 0:                  │  yield ERROR + continue
    │  altrimenti:                                     │  yield OK + prosegui
```

1. Il client apre un canale gRPC verso `localhost:50051`
2. Crea uno stub `CafeStub` e chiama `stub.OrderStream(generate_orders())`
3. `generate_orders()` è un generatore Python che produce i messaggi `OrderRequest` uno alla volta
4. Il server itera sull'`request_iterator` ricevuto: per ogni richiesta decide se aggiungere l'articolo, segnalare un errore o chiudere l'ordine
5. Il client itera sulla risposta dello stub per stampare ogni `OrderResponse` ricevuto

---

## File del progetto

### `cafe.proto`

Definisce il contratto gRPC. Contiene:

- **Servizio `Cafe`** con un'unica RPC:
  ```protobuf
  rpc OrderStream(stream OrderRequest) returns (stream OrderResponse) {}
  ```
  È un **bidirectional streaming RPC**: sia il client che il server inviano un flusso di messaggi contemporaneamente.

- **Messaggio `OrderRequest`** (richiesta del client):
  - `customer` (string): nome del cliente
  - `item` (string): nome del prodotto da ordinare
  - `quantity` (int32): quantità richiesta

- **Messaggio `OrderResponse`** (risposta del server):
  - `status` (string): `"OK"`, `"ERROR"` oppure `"SUMMARY"`
  - `message` (string): descrizione leggibile dell'evento
  - `partial_total` (double): totale accumulato fino a quel momento

---

### `server.py`

Implementa la logica del server gRPC.

**Prezzario (`PRICES`):** dizionario Python con i prodotti disponibili e i rispettivi prezzi:
```
caffe: 1.20 €
acqua: 0.80 €
cornetto: 1.50 €
cappuccino: 1.50 €
sigarette: 5.00 €
```

**Classe `CafeServicer`:** eredita da `cafe_pb2_grpc.CafeServicer` e implementa il metodo `OrderStream(self, request_iterator, context)`:

- Mantiene due variabili di stato locali: `total` (float) e `total_items` (int)
- Per ogni `request` nello stream:
  - Se `item == "fine"`: fa uno `yield` con `status="SUMMARY"`, restituisce il totale e termina con `return`
  - Se l'articolo non è nel listino oppure `qty <= 0`: fa uno `yield` con `status="ERROR"` e continua (`continue`) senza aggiornare il totale
  - Altrimenti: calcola il subtotale, aggiorna `total` e `total_items`, fa uno `yield` con `status="OK"`

**Avvio del server (`serve`):**
- Crea un server gRPC con `ThreadPoolExecutor(max_workers=10)`
- Registra il servicer con `add_CafeServicer_to_server`
- Esegue il bind su `[::]50051` (tutte le interfacce, porta 50051)
- Avvia il server e attende terminazione da tastiera con `wait_for_termination()`

---

### `client.py`

Implementa il client gRPC.

**Funzione `generate_orders()`:** generatore Python che produce una sequenza fissa di `OrderRequest`:
```python
orders = [
    ("caffe", 2),       # valido
    ("cornetto", 1),    # valido
    ("succo", 1),       # non presente nel listino → ERROR
    ("FINE", 0),        # chiude l'ordine → SUMMARY
]
```
Per ogni coppia `(item, qty)` crea e fa un `yield` di un `OrderRequest` con customer fisso `"Andrea"`.

**Funzione `run()`:**
- Apre un canale insicuro verso `localhost:50051`
- Crea lo stub `CafeStub`
- Chiama `stub.OrderStream(generate_orders())` — passa il generatore come stream di input
- Itera sulle risposte e stampa ciascuna in formato:
  ```
  [STATUS] messaggio | totale parziale: X.XX EUR
  ```

---

### `cafe_pb2.py` e `cafe_pb2_grpc.py`

File autogenerati da `protoc` (compilatore Protocol Buffers). Non vanno modificati a mano.

- `cafe_pb2.py`: contiene le classi Python corrispondenti ai messaggi `OrderRequest` e `OrderResponse`
- `cafe_pb2_grpc.py`: contiene la classe base `CafeServicer` (da cui eredita il server) e la classe `CafeStub` (usata dal client)

Per rigenerarli in caso di modifica al `.proto`:
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cafe.proto
```

---

## Requisiti

- Python 3
- Librerie gRPC per Python:

```bash
pip install grpcio grpcio-tools
```

---

## Esecuzione

Apri **due terminali** nella cartella `CafeRPC/`.

**Terminale 1 — Avvia il server:**
```bash
python3 server.py
```
Output atteso:
```
Server Calcolatrice avviato sulla porta: 50051
```

**Terminale 2 — Avvia il client:**
```bash
python3 client.py
```
Output atteso:
```
[OK] Prodotto caffe aggiunto all'ordine con quantità 2 | totale parziale: 2.40 EUR
[OK] Prodotto cornetto aggiunto all'ordine con quantità 1 | totale parziale: 3.90 EUR
[ERROR] Prodotto non valido oppure quantità errata: 1 | totale parziale: 3.90 EUR
[SUMMARY] Ordine terminato, prodotti ordinati: 2 , totale parziale: 3.9 | totale parziale: 3.90 EUR
```

---

## Concetti chiave

| Concetto | Dove si applica |
|---|---|
| Bidirectional streaming gRPC | `rpc OrderStream(stream ...) returns (stream ...)` in `cafe.proto` |
| Generatore Python come stream | `generate_orders()` in `client.py` passato direttamente allo stub |
| `yield` lato server | Il server produce risposte una alla volta man mano che arrivano le richieste |
| Stato mantenuto durante lo stream | `total` e `total_items` persistono per tutta la sessione di streaming |
| Gestione errori nel flusso | `continue` per saltare articoli non validi senza interrompere lo stream |
| Chiusura dello stream | `return` dopo il `yield SUMMARY` chiude il generator lato server |
