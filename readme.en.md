# Radar Combustível — MongoDB → Redis Streaming Pipeline

> **MBA FIAP in Technology | In-Memory Databases**
> Use case: **Radar Combustível Platform** (prices, gas stations, searches, ratings)
> Based on the `commithouse/lab-streaming-mongo-redis` lab (Restaurant Marketplace), scaled to **500 thousand documents** (100k per collection × 5) anchored in **125 real Brazilian cities**.

Detailed data modeling at [`docs/modeling-radar-combustivel.en.md`](docs/modeling-radar-combustivel.en.md)

Detailed streaming pipeline at [`docs/streaming-mongo-redis.en.md`](docs/streaming-mongo-redis.en.md)

---

## 1. Overview

The **Radar Combustível** platform monitors gas stations, prices, location, user searches and interactions (check-in, sharing, rating). This solution transforms that data — stored in **MongoDB** — into a **Redis serving layer** capable of answering in a few milliseconds questions such as:

- Which stations have the **lowest price** by region / city?
- Which fuels are **trending up** (positive 24h variation)?
- Which neighborhoods generate the **highest search volume**?
- Which stations had the **biggest recent price variation**?
- Which stations are **closest to the user** selling a specific fuel?
- Which stations have the **highest rating** and most check-ins?

A **Python pipeline** reads documents from MongoDB, turns them into events and updates Redis structures (Hashes, Sorted Sets, Geo, TimeSeries and RediSearch). A **Streamlit** dashboard consumes only Redis to deliver (near) real-time visualizations.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       MongoDB 7                             │
│  radar_combustivel  (replica set rs0 — 500k docs total)     │
│  ├─ postos                 (100k)                           │
│  ├─ localizacoes_postos    (100k)                           │
│  ├─ eventos_preco          (100k)                           │
│  ├─ buscas_usuarios        (100k)                           │
│  └─ avaliacoes_interacoes  (100k)                           │
└──────────────────────────┬──────────────────────────────────┘
        backfill (find batch) │       Change Stream (col.watch)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        Python Pipeline  ─  pipeline/mongodb_consumer.py     │
│  - Parallel backfill in batches of 2,000 docs               │
│  - 5 Change Stream threads (one per collection)             │
│  - Exponential reconnect + metrics in pipeline:metrics      │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Redis Stack 7.4 (serving)                  │
│  Hash    posto:{id}                — gas station profile    │
│  Hash    posto:{id}:precos         — current price / fuel   │
│  Hash    posto:{id}:rating         — sum/count/util/checkin │
│  GEO     geo:postos                — global location        │
│  GEO     geo:postos:{fuel}         — only sellers of fuel   │
│  ZSET    rank:preco:{fuel}:{UF}    — lowest price by state  │
│  ZSET    rank:preco:{fuel}:cidade  — lowest price by city   │
│  ZSET    rank:preco:{fuel}:global  — lowest price global    │
│  ZSET    rank:variacao:{fuel}:24h  — biggest swings 24h     │
│  ZSET    rank:buscas:{bairro|...}  — top searches (global)  │
│  ZSET    rank:buscas:bairro:{fuel} — top neighborhoods/fuel │
│  ZSET    rank:buscas:cidade:{fuel} — top cities by fuel     │
│  ZSET    rank:buscas:fuel:{UF}...  — most searched fuels    │
│  ZSET    rank:postos:{rating|...}  — engagement rankings    │
│  TS      ts:posto:{id}:{fuel}      — price history          │
│  TS      ts:avg:{fuel}:{UF}        — state average          │
│  TS      ts:buscas:total / lat.    — volume and latency     │
│  IDX     idx:postos (RediSearch)   — full-text + tag + geo  │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│   Streamlit Dashboard  ─  queries/data-view.py              │
│   • Overview / pipeline health                              │
│   • Prices, rankings, 24h variation and time series         │
│   • GEOSEARCH map (configurable radius)                     │
│   • Behavior (top neighborhoods / cities / fuels)           │
│   • Ratings and interactions                                │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Repository structure

```
radar-combustivel/
├── docker-compose.yml                 # MongoDB rs0 + Redis Stack
├── requirements.txt                   # Python dependencies (runtime)
├── .env.example                       # Variables template
├── readme.md                          # Portuguese version
├── readme.en.md                       # (this file)
├── docs/
│   ├── modelagem-radar-combustivel.md # Schemas, Redis keys, access patterns (PT)
│   ├── streaming-mongo-redis.md       # Pipeline details (PT)
│   ├── modeling-radar-combustivel.en.md
│   ├── streaming-mongo-redis.en.md
│   └── arquitetura_final.png
├── init/
│   ├── seed.py                        # Populate Mongo (500k docs in 125 real cities)
│   ├── redis_indexes.py               # Create RediSearch index idx:postos
│   └── insert_test_avaliacoes.py      # Generate new ratings to test Change Stream
├── pipeline/
│   ├── config.py                      # Variables and Redis key catalog
│   ├── event_transformer.py           # Mongo doc → Redis commands
│   └── mongodb_consumer.py            # Backfill + Change Streams
└── queries/
    ├── redis_reader.py                # Read functions + CLI demo
    └── data-view.py                   # Streamlit dashboard
```

---

## 4. Environment setup

### 4.1 Prerequisites

- Docker + Docker Compose
- Python 3.10+
- (Optional) MongoDB Compass and RedisInsight

### 4.2 Environment variables

```bash
cp .env.example .env
```

Defaults match the docker-compose. For **500 thousand documents** (100k per collection) the key variables are:

| Variable | Default | Why |
|---|---|---|
| `BACKFILL_BATCH_SIZE` | `2000` | Light pressure on Redis, keeps pipeline with 50–80 batches in parallel |
| `BACKFILL_WORKERS` | `4` | 4 threads writing to Redis in pipeline; local benchmark ~1,500 docs/s |
| `TS_RETENTION_MS` | `30 days` | Enough price history for trend analysis |
| `RANKING_WINDOW_HOURS` | `24` | Window used for price variation % |
| `STREAMLIT_REFRESH_SECONDS` | `10` | Dashboard auto-refresh default |

### 4.3 Bring up the infrastructure

```bash
docker compose up -d
```

The `mongo-init` service initializes the `rs0` replica set automatically (required for Change Streams).

### 4.4 Install Python dependencies

```bash
python -m venv .venv
.\.venv\Scripts\activate          # Windows
# or: source .venv/bin/activate   # Linux/macOS
pip install -r requirements.txt
```

### 4.5 Seed MongoDB

```bash
python init/seed.py --drop
```

Creates 500,000 synthetic documents spread across 125 real Brazilian cities (all 27 states covered), with coordinates near official city centers and schemas as in `docs/modeling-radar-combustivel.en.md`.

### 4.6 Create the RediSearch index

```bash
python init/redis_indexes.py
```

Creates the `idx:postos` index (FT.CREATE) over `doc:posto:*` and initializes the `pipeline:metrics` hash with default values. All other structures (Hashes, Sorted Sets, Geo, TimeSeries) are created on demand by `event_transformer.py`.

---

## 5. Running the pipeline

### Terminal 1 — Consumer (backfill + Change Stream)

```bash
python -m pipeline.mongodb_consumer
```

Expected output (abridged):

```
[backfill] postos: 100,000 docs in ~70s (~1,400 docs/s)
[backfill] localizacoes_postos: 100,000 docs in ~70s (~1,400 docs/s)
[backfill] eventos_preco: 100,000 docs in ~130s (~770 docs/s)
[backfill] buscas_usuarios: 100,000 docs in ~80s (~1,250 docs/s)
[backfill] avaliacoes_interacoes: 100,000 docs in ~110s (~900 docs/s)
[watch] opening Change Stream on postos
[watch] opening Change Stream on localizacoes_postos
[watch] opening Change Stream on eventos_preco
[watch] opening Change Stream on buscas_usuarios
[watch] opening Change Stream on avaliacoes_interacoes
metrics: total=500000 errors=0 last=avaliacoes_interacoes
```

> Use `SKIP_BACKFILL=1` on subsequent runs to skip the backfill and go straight to streaming.

### Terminal 2 — Demo queries

```bash
python queries/redis_reader.py
```

### Terminal 3 — Streamlit dashboard

```bash
streamlit run queries/data-view.py
```

Open `http://localhost:8501`. The five tabs (Overview, Prices, Map & Geo, Behavior, Ratings) consume **only Redis** and refresh automatically.

### Terminal 4 — Change Stream test

```bash
python init/insert_test_avaliacoes.py
# or targeting a specific station:
python init/insert_test_avaliacoes.py --posto <ObjectId>
```

Inserts 14 synthetic documents (10 ratings + 3 check-ins + 1 share) with varied scores into `avaliacoes_interacoes` to demonstrate end-to-end propagation:

- Snapshot the Ratings tab before running;
- Execute the script;
- Watch terminal 1 (`mongodb_consumer.py`) process the 14 events;
- See the dashboard react (station average drops, check-in and share counters grow).

> The same test can be done by inserting documents directly in MongoDB via `mongosh`.

---

## 6. Redis structures adopted

| Structure | Key | Why we picked it |
|---|---|---|
| **Hash** | `posto:{id}` | O(1) access to the full profile; ideal to hydrate tables |
| **Hash** | `posto:{id}:precos` | one field per fuel, atomic update via `HSET` |
| **Hash** | `posto:{id}:rating` | `sum + count` for correct average under concurrency |
| **Sorted Set** | `rank:preco:{fuel}:{UF\|city\|global}` | `ZRANGE 0 N` returns lowest price in O(log N) |
| **Sorted Set** | `rank:variacao:{fuel}:24h` | shows swings via `ZREVRANGE`/`ZRANGE` |
| **Sorted Set** | `rank:buscas:{bairro\|cidade\|uf\|combustivel}` | global rankings with atomic `ZINCRBY` |
| **Sorted Set** | `rank:buscas:bairro:{fuel}` | top neighborhoods by fuel (fuel+UF+city filter) |
| **Sorted Set** | `rank:buscas:cidade:{fuel}` | top cities by fuel |
| **Sorted Set** | `rank:buscas:fuel:{UF}` and `rank:buscas:fuel:{UF}\|{city}` | most searched fuels per region |
| **Sorted Set** | `rank:postos:{rating\|util\|checkin\|compartilhamento}` | ordered engagement metrics |
| **GEO** | `geo:postos`, `geo:postos:{fuel}` | `GEOSEARCH BYRADIUS` for proximity queries |
| **TimeSeries** | `ts:posto:{id}:{fuel}` | price evolution with native window aggregation |
| **TimeSeries** | `ts:avg:{fuel}:{UF}` | state average for comparative charts |
| **TimeSeries** | `ts:buscas:total`, `ts:buscas:latencia` | search funnel observability |
| **RediSearch** | `idx:postos` | full-text on name / city / neighborhood + TAG filters (UF, brand) and geo |

---

## 7. Architecture decisions

| Decision | Choice | Rationale |
|---|---|---|
| Event capture | MongoDB Change Stream with `full_document=updateLookup` | Avoids polling and delivers the full document to the transformer |
| Backfill | `find()` cursor + `ThreadPoolExecutor` (4 workers, 2k batches) | Loads 500k docs in ~17 minutes without saturating Redis |
| Redis pipeline | `redis-py` `pipeline(transaction=False)` per event | Cuts RTT latency up to 5× when there are multiple commands |
| Ranking atomicity | `ZADD` (price) + `ZINCRBY` (searches) | Native atomic ops in O(log N) |
| Rating average | `sum` and `count` kept separate in Hash, average recomputed and stored via `ZADD` | Avoids race condition between parallel threads |
| 24h variation | `ZADD` replaces previous score | Latest variation prevails — coherent with "most recent" |
| Multi-dimensional rankings | 8 sorted sets per search event (4 global + 4 fuel/region-specific) | Dashboard filters answered without in-memory post-processing |
| TimeSeries | 30-day retention + `DUPLICATE_POLICY LAST` | Fast recovery and idempotency on replay |
| Observability | `pipeline:metrics` Hash flushed every 15s | Dashboard shows pipeline health without hitting Mongo |
| Reconnect | Exponential backoff up to 30s | Pipeline auto-recovers from Mongo/Redis drops |
| Redis cache | `maxmemory 1.5GB`, `allkeys-lru`, AOF on | Enough for 500k docs + indexes + 30 days of TS |
| Realistic dataset | `seed.py` anchors 100% of data on 125 Brazilian cities with IBGE codes + coordinates | GEOSEARCH coherent with cadastre, dashboard filters return non-empty results |

---

## 8. Highlights

- **Realistic dataset** with 125 real Brazilian cities (IBGE codes, coordinates), covering all 27 states.
- **Geographic** queries via `GEOSEARCH` per fuel.
- **Time series** of price per station and state average, with native aggregation.
- **Multi-dimensional rankings** of searches (fuel × UF × city) written at event time.
- **Parallel backfill** vs streaming `col.watch` — the same transformer serves both modes.
- **Observability**: pipeline records throughput, last event, errors and last collection type in `pipeline:metrics`.
- **Failure handling**: exponential reconnect on Change Streams; `apply_event` is idempotent.
- **Multi-view** Streamlit with filters (fuel/UF/city), interactive map (`pydeck`), charts (`plotly`) and auto-refresh.
- **City sidebar** dynamically populated from Redis keys (only cities actually present).
- **Hierarchical auto-center** of the map (city → UF → global) with adaptive default radius.

---

## 9. Useful commands

```bash
# wipe everything
docker compose down -v

# inspect Redis (CLI)
docker exec -it radar-redis redis-cli
> HGETALL pipeline:metrics
> ZRANGE rank:preco:gasolina_comum:SP 0 9 WITHSCORES
> ZREVRANGE rank:buscas:bairro:gasolina_comum 0 9 WITHSCORES
> GEOSEARCH geo:postos FROMLONLAT -46.6333 -23.5505 BYRADIUS 5 km ASC WITHCOORD COUNT 20
> TS.RANGE ts:buscas:total - + AGGREGATION SUM 3600000
> FT.SEARCH idx:postos "@bandeira:{Shell} @uf:{SP} @ativo:[1 1]"

# inspect Mongo (CLI)
docker exec -it radar-mongo mongosh
> use radar_combustivel
> db.eventos_preco.countDocuments()
> db.avaliacoes_interacoes.find({posto_id: ObjectId("...")}).pretty()
```

---

## 10. Validation checklist

- [ ] `docker compose up -d` brings up Mongo (rs0) and Redis Stack without errors.
- [ ] `python init/seed.py --drop` populates Mongo (500k docs in 125 real cities).
- [ ] `python init/redis_indexes.py` creates `idx:postos` without error.
- [ ] `python -m pipeline.mongodb_consumer` runs the backfill and opens 5 Change Streams.
- [ ] `python queries/redis_reader.py` prints populated rankings.
- [ ] `streamlit run queries/data-view.py` opens the five tabs with charts.
- [ ] Inserting a new document in `eventos_preco` via MongoDB is reflected in the dashboard in < 5s; and/or
- [ ] `python init/insert_test_avaliacoes.py` inserts new ratings for a station and the change shows up in the dashboard.

---

## 11. Notes

The scripts `redis_indexes_radar_batch.py` and `redis_indexes_radar_batch_checkpoint.py` are **not used** in this solution because the Docker issue they were meant to work around was resolved by tuning configuration and execution order.

---

## 12. References

- MongoDB Change Streams — https://www.mongodb.com/docs/manual/changeStreams/
- Redis Sorted Sets — https://redis.io/docs/data-types/sorted-sets/
- RediSearch — https://redis.io/docs/interact/search-and-query/
- RedisTimeSeries — https://redis.io/docs/data-types/timeseries/
- Base lab (Restaurant Marketplace) — https://github.com/commithouse/lab-streaming-mongo-redis
