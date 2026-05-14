# MongoDB → Redis Streaming Pipeline (Radar Combustível)

This document describes in detail the pipeline flow built for the **Radar Combustível** case and the decisions made to support a database with **500 thousand documents** distributed across five collections (10× larger than the original Restaurant Marketplace lab).

---

## 1. Source collections

| Collection | Role in the domain | Typical volume |
|---|---|---|
| `postos` | Master record of gas stations (CNPJ, brand, address, location) | ~100k |
| `localizacoes_postos` | Normalized address + neighborhood + IBGE + GeoJSON Point | ~100k |
| `eventos_preco` | Immutable history of price changes per fuel | ~100k |
| `buscas_usuarios` | Telemetry for each search (radius, filters, latency, search center) | ~100k |
| `avaliacoes_interacoes` | Ratings, comments, check-ins, shares, util_count | ~100k |

> The pipeline was tuned for that volume, but it scales linearly: `BACKFILL_BATCH_SIZE` × `BACKFILL_WORKERS` controls throughput.

---

## 2. Flow view

```
                ┌─────────────────────────────┐
                │  MongoDB (rs0, oplog)       │
                └────────────┬────────────────┘
                             │
       ┌─────────────────────┼─────────────────────────┐
       │                     │                         │
       ▼                     ▼                         ▼
   Backfill            Change Stream          Change Stream
   find() + batch     (postos, locs)          (eventos_preco,
                                              buscas, avaliacoes)
       │                     │                         │
       └─────────────────────┴─────────────────────────┘
                             │
                             ▼
                ┌─────────────────────────────┐
                │  event_transformer.py       │
                │  apply_event(col, doc)      │
                └────────────┬────────────────┘
                             ▼
                ┌─────────────────────────────┐
                │  Redis Stack 7.4            │
                │  Hash / GEO / ZSET / TS /   │
                │  RediSearch                 │
                └────────────┬────────────────┘
                             ▼
                ┌─────────────────────────────┐
                │  Streamlit (data-view.py)   │
                └─────────────────────────────┘
```

---

## 3. Batch backfill

The backfill is implemented in `pipeline/mongodb_consumer.py:run_backfill`. Key decisions:

1. **Processing order.** Collections `postos` and `localizacoes_postos` are processed first because other collections (especially `eventos_preco`) depend on the city/UF/coordinates fields stored in the `posto:{id}` Hash to compute the rankings.
2. **Batches of 2,000 documents.** We keep Mongo cursors open with `batch_size(2000)` and accumulate documents in a local queue. When the queue reaches `BACKFILL_BATCH_SIZE`, the batch is submitted to the `ThreadPoolExecutor`.
3. **4 parallel workers.** Each worker opens its own Redis connection and uses `redis.pipeline(transaction=False)` to send multiple writes in a single round-trip.
4. **Throttling.** When the number of pending futures exceeds `BACKFILL_WORKERS × 2`, we stop submitting new batches until they drain — avoiding memory pressure during the load.
5. **Idempotency.** Since `apply_event` uses `HSET`, `ZADD`, `GEOADD` and `TS.ADD ON_DUPLICATE LAST`, running the backfill twice yields the same final state.

Typical throughput on a laptop: **900–1,500 docs/s** per collection, totaling around **17 minutes** for 500k documents.

---

## 4. Streaming via Change Stream

`run_change_streams` opens **5 threads** (one per collection) running `col.watch` with the pipeline `[{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]` and `full_document="updateLookup"`. The full document arrives in the event, so the transformer does not need a follow-up query.

Errors and disconnections are handled with **exponential backoff** (1s, 2s, 4s … up to 30s). The loop is interrupted only by the signal handler (SIGINT/SIGTERM).

> Because it relies on the oplog, MongoDB must be in **replica set** mode. The `mongo-init` service in `docker-compose.yml` runs `rs.initiate(...)` automatically.

---

## 5. Mongo → Redis mapping

### 5.1 `postos`
- `HSET posto:{id}` complete profile (cnpj, brand, address…)
- `HSET doc:posto:{id}` mirror indexed by RediSearch (`idx:postos`)
- `GEOADD geo:postos lon lat {id}`

### 5.2 `localizacoes_postos`
- Updates `posto:{id}` with `bairro`, `municipio`, `uf`, `codigo_ibge`, `lat`, `lon`
- Updates `doc:posto:{id}` (search index)
- Updates `geo:postos`

### 5.3 `eventos_preco`
- `HSET posto:{id}:precos {fuel} preco_novo` + `… {fuel}:updated_at ts`
- `ZADD rank:preco:{fuel}:{UF} preco {posto}`
- `ZADD rank:preco:{fuel}:cidade:{cidade} preco {posto}`
- `ZADD rank:preco:{fuel}:global preco {posto}`
- `ZADD rank:variacao:{fuel}:24h variacao_pct {posto}`
- `GEOADD geo:postos:{fuel} lon lat {posto}` (only stations that sell the fuel)
- `TS.ADD ts:posto:{id}:{fuel} ts preco`
- `TS.ADD ts:avg:{fuel}:{UF} ts preco` (used for the `TS.RANGE … AGGREGATION AVG` average)

### 5.4 `buscas_usuarios`
- `ZINCRBY rank:buscas:bairro 1 "{UF}|{cidade}|{bairro}"`
- `ZINCRBY rank:buscas:cidade 1 "{UF}|{cidade}"`
- `ZINCRBY rank:buscas:uf 1 "{UF}"`
- `ZINCRBY rank:buscas:combustivel 1 "{combustivel}"`
- `ZINCRBY rank:buscas:bairro:{fuel} 1 "{UF}|{cidade}|{bairro}"`
- `ZINCRBY rank:buscas:cidade:{fuel} 1 "{UF}|{cidade}"`
- `ZINCRBY rank:buscas:fuel:{UF} 1 "{combustivel}"`
- `ZINCRBY rank:buscas:fuel:{UF}|{cidade} 1 "{combustivel}"`
- `TS.INCRBY ts:buscas:total 1 TIMESTAMP {ts}`
- `TS.ADD ts:buscas:latencia {ts} {latencia_ms}`

### 5.5 `avaliacoes_interacoes`
- `HINCRBYFLOAT posto:{id}:rating sum nota` + `HINCRBY count 1`
- Recomputes `media = sum / count` and `ZADD rank:postos:rating media {posto}`
- `ZINCRBY rank:postos:util util_count {posto}` (only when `util_count > 0`)
- `ZINCRBY rank:postos:checkin 1 {posto}` when `tipo == "check_in"`
- `ZINCRBY rank:postos:compartilhamento 1 {posto}` when `tipo == "compartilhamento"`

---

## 6. Why each structure?

| Business question | Chosen structure | Reason |
|---|---|---|
| Cheapest station | Sorted Set (`rank:preco:…`) | `ZRANGE 0 N` is O(log N + N) |
| Biggest 24h up/down | Sorted Set by variation | Keeps latest value; `ZREVRANGE` for up, `ZRANGE` for down |
| Nearby stations | GEO (`geo:postos`) | Native `GEOSEARCH BYRADIUS` |
| Nearby stations with fuel X | GEO per fuel | Filters only sellers of the fuel, avoids costly post-filtering |
| Price history | TimeSeries | `AVG`/`SUM` window aggregation already in Redis |
| Search volume and latency | TimeSeries | Operational / observability |
| Top neighborhoods / cities / fuel | Sorted Set | Atomic `ZINCRBY` |
| Multi-dimensional search rankings | Sorted Set per fuel + region | Dashboard filters answered without post-processing |
| Average rating | Hash (sum/count) + Sorted Set (average) | Allows incremental update without recomputing everything |
| Profile / hydration | Hash (`posto:{id}`) | O(1) lookup to enrich rankings in the UI |
| Search by name / brand / city | RediSearch (`idx:postos`) | Full-text + TAG/NUM/GEO filters in a single call |

---

## 7. Observability

`mongodb_consumer.py` keeps in-memory counters (`Counters`) and every `METRICS_INTERVAL_SECONDS` performs `HSET pipeline:metrics`. The dashboard reads this hash to display:

- Total events processed
- Errors since process start
- Last collection updated and timestamp
- Events per collection (bar chart)
- Search volume (`ts:buscas:total`)
- Average latency (`ts:buscas:latencia`)

---

## 8. Failure handling

- Exponential reconnect on every watcher (`backoff = min(backoff*2, 30)`).
- `apply_event` wraps per-document errors and increments `errors` without taking the pipeline down.
- `TS.ADD … ON_DUPLICATE LAST` avoids failures on replay.
- Backfill is restartable: Redis structures are deterministic for the same input.

---

## 9. Batch vs streaming comparison

| Aspect | Backfill (batch) | Change Stream |
|---|---|---|
| Latency | Seconds to minutes | Milliseconds (Mongo → Redis ops ≤ 50ms on laptop) |
| Computational cost | High and short | Low and continuous |
| When to use | Initialization or rehydration | Continuous updates |
| Implementation | `find()` + ThreadPoolExecutor | `col.watch(...)` + thread per collection |

The solution combines both: a **boot-time backfill** ensures the correct state for the 500k pre-existing documents, and **Change Streams** keep Redis in sync with any new `insert/update/replace`.

---

## 10. Suggested next steps

- Shard by UF/city on ranking keys if cardinality explodes.
- Replace average computation via Hash with **RedisJSON + JSON.SET** with CAS expressions.
- Publish metrics to **Prometheus** and add a Grafana dashboard.
- Additional RediSearch filters (dynamic radius, price range, sort by rating).
