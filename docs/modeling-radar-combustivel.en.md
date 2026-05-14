# Radar Combustível Modeling — MongoDB and Redis

> Companion document to [`streaming-mongo-redis.en.md`](streaming-mongo-redis.en.md).
> That document describes *how* data flows (pipeline, Change Stream, observability).
> This one describes *what* the data looks like (entities, schemas, keys, access patterns)
> and *why* each modeling decision was made.

---

## 1. Domain context

The Radar Combustível Platform has five main entities. All have native MongoDB `_id` ObjectIds and relate by reference (no embedding):

```
┌──────────┐                        ┌──────────────────────────┐
│ postos   │───────1:1──────────────│ localizacoes_postos      │
│          │                        │                          │
│          │───────1:N──────────────│ eventos_preco            │
│          │                        │                          │
│          │───────1:N──────────────│ avaliacoes_interacoes    │
└──────────┘                        └──────────────────────────┘
       ▲                                          ▲
       │                                          │
       │ location filter                          │ station filter
       │                                          │
┌──────┴──────────────────────────────────────────┴───────┐
│              buscas_usuarios                            │
│  (refers to geo_centro, cidade, estado, combustivel)    │
└─────────────────────────────────────────────────────────┘
```

**Why not embed?** Each collection has different cardinality and access profile:

- `postos` is a static cadastre (rare writes, frequent reads).
- `eventos_preco` grows continuously (constant writes, reads by time windows).
- `buscas_usuarios` also grows continuously, but is denormalized (already carries city/state to avoid lookups).

Embedding `eventos_preco` inside `postos` would make documents grow indefinitely and break the oplog optimization for Change Streams.

---

## 2. MongoDB schemas

### 2.1 `postos`

Master record. One document per station, updated rarely (registration, brand change, shutdown).

```json
{
  "_id": ObjectId(),
  "cnpj": "12.345.678/0001-90",
  "nome_fantasia": "Posto Bom Vista EIRELI",
  "bandeira": "Petrobras | Shell | Ipiranga | Raizen | Ale | Bandeira Branca",
  "endereco": {
    "logradouro": "Av. Brasil",
    "numero": "1234",
    "bairro": "Centro",
    "cep": "37500000",
    "cidade": "Itajuba",
    "estado": "MG"
  },
  "telefone": "(35) 1234-5678",
  "ativo": true,
  "location": { "type": "Point", "coordinates": [lon, lat] },
  "created_at": ISODate(),
  "updated_at": ISODate()
}
```

**Indexes**:
- `2dsphere` on `location` (geospatial queries in Mongo before the serving layer exists).

**Volume**: ~100k documents.

### 2.2 `localizacoes_postos`

Normalized mirror of the location, with official IBGE code. Exists separately because it can be updated by geographic integrations (IBGE, ANP) without touching the cadastre.

```json
{
  "_id": ObjectId(),
  "posto_id": ObjectId(),
  "municipio": "Itajuba",
  "bairro": "Centro",
  "uf": "MG",
  "codigo_ibge": "3132404",
  "geo": { "type": "Point", "coordinates": [lon, lat] },
  "atualizado_em": ISODate()
}
```

**Indexes**:
- `2dsphere` on `geo`.

**Volume**: ~100k documents (1:1 with `postos`).

### 2.3 `eventos_preco`

Immutable history of price changes. Each document is a discrete event, never updated — only inserted.

```json
{
  "_id": ObjectId(),
  "posto_id": ObjectId(),
  "combustivel": "GASOLINA_COMUM | GASOLINA_ADITIVADA | ETANOL | DIESEL_S10 | DIESEL_S500 | GNV",
  "preco_anterior": 5.79,
  "preco_novo": 5.89,
  "variacao_pct": 1.7271,
  "unidade": "BRL_L",
  "fonte": "crawler | manual | api",
  "ocorrido_em": ISODate(),
  "revisado": true
}
```

**Indexes**:
- Compound `(posto_id, ocorrido_em DESC)` for per-station history.

**Volume**: ~100k documents.

### 2.4 `buscas_usuarios`

Telemetry. Each document represents a query made by a user (mobile app or web). Includes search center geolocation, filters and operational metrics (latency, result count).

```json
{
  "_id": ObjectId(),
  "usuario_id": "uuid",
  "session_id": "uuid",
  "tipo_combustivel": "GASOLINA_COMUM",
  "cidade": "Itajuba",
  "estado": "MG",
  "raio_km": 5,
  "filtros": {
    "apenas_abertos": true,
    "ordenacao": "preco | distancia | avaliacao",
    "bairro": "Centro"
  },
  "geo_centro": { "type": "Point", "coordinates": [lon, lat] },
  "consultado_em": ISODate(),
  "resultado_count": 12,
  "latencia_ms": 87
}
```

**Indexes**:
- Compound `(estado, cidade)` for regional aggregation.

**Volume**: ~100k documents.

### 2.5 `avaliacoes_interacoes`

Social interactions with stations: ratings with score, comments, check-ins, shares. The `util_count` field is incremented by other users when they mark that interaction as helpful.

```json
{
  "_id": ObjectId(),
  "posto_id": ObjectId(),
  "usuario_id": "uuid",
  "tipo": "avaliacao | comentario | check_in | compartilhamento",
  "nota": 1 | 2 | 3 | 4 | 5 | null,
  "comentario": "string | null",
  "created_at": ISODate(),
  "util_count": 0
}
```

**Indexes**:
- Simple on `posto_id`.

**Volume**: ~100k documents.

---

## 3. Redis serving model

The Redis modeling was **query-driven**, not entity-driven. Each dashboard access pattern dictates a structure optimized for that pattern. General rule: **one structure per question**.

### 3.1 Key conventions

All keys follow the pattern `<category>:<entity>[:<dimension>]`, built in code by the `pipeline/config.RedisKeys` module to avoid duplication and typos. Dimensions are separated by `:` when hierarchical, and by `|` when composite inside a Sorted Set member.

| Pattern | Meaning |
|---|---|
| `posto:{id}` | Hash with a station profile |
| `geo:postos[:{fuel}]` | Global or per-fuel geo index |
| `rank:<attribute>:<dimension>` | Sorted Set for ordered ranking |
| `ts:<entity>:<dimension>` | TimeSeries with configured retention |
| `idx:postos` | The only RediSearch index (FT.CREATE) |
| `doc:posto:{id}` | Mirror Hash indexed by RediSearch |
| `pipeline:metrics` | Hash of consumer telemetry |

UF codes always upper-case (`SP`, `MG`). Fuels lower-case in keys (`gasolina_comum`), upper-case in values and the fuel rankings.

### 3.2 Hashes

```
posto:{id}
  posto_id        ObjectId-string (mirrored for indexing)
  cnpj            string
  nome_fantasia   string
  bandeira        string
  bairro, cidade, estado, uf, cep, logradouro, numero, telefone
  ativo           "0" | "1"
  lat, lon        float-string
  codigo_ibge     string
  updated_at      epoch_ms-string

posto:{id}:precos
  GASOLINA_COMUM             float-string (current price)
  GASOLINA_COMUM:updated_at  epoch_ms-string
  ETANOL                     float-string
  ETANOL:updated_at          epoch_ms-string
  ...

posto:{id}:rating
  sum       float-string (sum of scores)
  count     int-string (number of ratings)
  util      int-string (sum of util_count)
  checkin   int-string (number of check-ins)
  compart   int-string (number of shares)

doc:posto:{id}
  Same structure as posto:{id}, indexed by idx:postos.
```

**Why Hash?** O(1) access per field and per key, enabling table and map hydration from IDs coming from rankings without a second round-trip to MongoDB.

### 3.3 Sorted Sets

```
# Price — score = current price in BRL
rank:preco:{fuel}:global              {member: posto_id, score: price}
rank:preco:{fuel}:{UF}                {member: posto_id, score: price}
rank:preco:{fuel}:cidade:{city}       {member: posto_id, score: price}

# 24h variation — score = most recent variacao_pct
rank:variacao:{fuel}:24h              {member: posto_id, score: variacao_pct}

# Global search rankings — score = total searches
rank:buscas:bairro                    {member: "UF|city|neighborhood", score: count}
rank:buscas:cidade                    {member: "UF|city", score: count}
rank:buscas:uf                        {member: "UF", score: count}
rank:buscas:combustivel               {member: fuel, score: count}

# Multi-dimensional search rankings — fuel-specific
rank:buscas:bairro:{fuel}             {member: "UF|city|neighborhood", score: count}
rank:buscas:cidade:{fuel}             {member: "UF|city", score: count}

# Most searched fuels per region
rank:buscas:fuel:{UF}                 {member: fuel, score: count}
rank:buscas:fuel:{UF}|{city}          {member: fuel, score: count}

# Station engagement
rank:postos:rating                    {member: posto_id, score: average}
rank:postos:util                      {member: posto_id, score: sum_util_count}
rank:postos:checkin                   {member: posto_id, score: total_check_ins}
rank:postos:compartilhamento          {member: posto_id, score: total_shares}
```

**Why Sorted Set?** `ZRANGE` in O(log N + k) for top-N by score, atomic `ZINCRBY` for counters, and `ZADD` enables score updates while keeping the correct position automatically.

### 3.4 Geo

```
geo:postos                    Geo of every station (any fuel)
geo:postos:gasolina_comum     Geo only for stations selling gasolina_comum
geo:postos:etanol             Geo only for stations selling ethanol
...
```

**Why one key per fuel?** Avoids post-filtering. The query "stations near me that sell Diesel S10" becomes a single `GEOSEARCH geo:postos:diesel_s10 ...` rather than fetching all and filtering in memory.

### 3.5 TimeSeries

```
ts:posto:{id}:{fuel}          Price history of a station/fuel
  Labels: posto={id}, combustivel={fuel}, uf={UF}
  Retention: 30 days
  DUPLICATE_POLICY: LAST

ts:avg:{fuel}:{UF}            Raw prices per UF (for AVG aggregation via TS.RANGE)
  Labels: combustivel={fuel}, uf={UF}, kind=raw

ts:buscas:total               Incremental counter of search volume
  Labels: kind=buscas_total

ts:buscas:latencia            Latency of each individual search
  Labels: kind=buscas_latencia_ms
```

**Why TimeSeries?** Native window aggregation in Redis (`TS.RANGE ... AGGREGATION AVG 60000`). Without TimeSeries we would need to maintain lists and compute averages in memory in the consumer.

### 3.6 RediSearch — the only formal index

```
FT.CREATE idx:postos
  ON HASH
  PREFIX 1 doc:posto:
  SCHEMA
    nome_fantasia  TEXT     WEIGHT 2.0  SORTABLE
    bandeira       TAG                  SORTABLE
    cidade         TEXT                 SORTABLE
    bairro         TEXT                 SORTABLE
    estado         TAG                  SORTABLE
    uf             TAG                  SORTABLE
    ativo          NUMERIC
    lat            NUMERIC
    lon            NUMERIC
```

**Why RediSearch?** The only path to combine full-text with TAG, numeric and geo filters in a single low-latency call. Supports expressive syntax like `@cuisine:{pizza} @bairro:{Pinheiros} @stars:[4.5 5]`.

---

## 4. Per-event Mongo → Redis mapping

Each Mongo document insert/update triggers a specific set of Redis commands. Everything idempotent.

### 4.1 Insert in `postos`
```
HSET posto:{id}      cnpj nome_fantasia bandeira ... lat lon
HSET doc:posto:{id}  <mirror>                           (RediSearch indexes)
GEOADD geo:postos    lon lat {id}
```

### 4.2 Insert in `localizacoes_postos`
```
HSET posto:{id}      municipio bairro uf codigo_ibge lat lon
HSET doc:posto:{id}  <same fields>
GEOADD geo:postos    lon lat {id}
```

### 4.3 Insert in `eventos_preco`
```
HSET   posto:{id}:precos             {fuel} preco_novo
HSET   posto:{id}:precos             {fuel}:updated_at ts_ms

ZADD   rank:preco:{fuel}:{UF}        preco {id}
ZADD   rank:preco:{fuel}:cidade:{cidade} preco {id}
ZADD   rank:preco:{fuel}:global      preco {id}
ZADD   rank:variacao:{fuel}:24h      variacao_pct {id}

GEOADD geo:postos:{fuel}             lon lat {id}

TS.ADD ts:posto:{id}:{fuel}          ts_ms preco
TS.ADD ts:avg:{fuel}:{UF}            ts_ms preco
```

### 4.4 Insert in `buscas_usuarios`
```
ZINCRBY rank:buscas:bairro                    1 "UF|city|neighborhood"
ZINCRBY rank:buscas:cidade                    1 "UF|city"
ZINCRBY rank:buscas:uf                        1 "UF"
ZINCRBY rank:buscas:combustivel               1 fuel

ZINCRBY rank:buscas:bairro:{fuel}             1 "UF|city|neighborhood"
ZINCRBY rank:buscas:cidade:{fuel}             1 "UF|city"
ZINCRBY rank:buscas:fuel:{UF}                 1 fuel
ZINCRBY rank:buscas:fuel:{UF}|{cidade}        1 fuel

TS.INCRBY ts:buscas:total                     1     TIMESTAMP ts_ms
TS.ADD    ts:buscas:latencia                  ts_ms latencia_ms
```

### 4.5 Insert in `avaliacoes_interacoes`
```
# if has score:
HINCRBYFLOAT posto:{id}:rating  sum   score
HINCRBY      posto:{id}:rating  count 1
ZADD         rank:postos:rating media {id}    (recompute = sum/count)

# if util_count > 0:
HINCRBY      posto:{id}:rating  util  util_count
ZINCRBY      rank:postos:util   util_count {id}

# if tipo == check_in:
HINCRBY      posto:{id}:rating       checkin 1
ZINCRBY      rank:postos:checkin     1 {id}

# if tipo == compartilhamento:
HINCRBY      posto:{id}:rating              compart 1
ZINCRBY      rank:postos:compartilhamento   1 {id}
```

---

## 5. Access pattern × chosen structure

| User question | Structure used | Sample command |
|---|---|---|
| "Where is the cheapest gas in SP?" | Sorted Set `rank:preco:gasolina_comum:SP` | `ZRANGE 0 9 WITHSCORES` |
| "Lowest ethanol price in Itajubá?" | Sorted Set `rank:preco:etanol:cidade:Itajuba` | `ZRANGE 0 0 WITHSCORES` |
| "Who raised Diesel S10 the most in the last 24h?" | Sorted Set `rank:variacao:diesel_s10:24h` | `ZREVRANGE 0 9 WITHSCORES` |
| "Ethanol stations within 5 km from me" | Geo `geo:postos:etanol` | `GEOSEARCH FROMLONLAT lon lat BYRADIUS 5 km` |
| "How did station X's gas price change in the month?" | TimeSeries `ts:posto:{id}:gasolina_comum` | `TS.RANGE - + AGGREGATION AVG 3600000` |
| "Which MG neighborhoods search diesel the most?" | Sorted Set `rank:buscas:bairro:diesel_s10` + prefix `MG|` | `ZREVRANGE 0 999` + filter |
| "Most searched fuels in Itajubá?" | Sorted Set `rank:buscas:fuel:MG|Itajuba` | `ZREVRANGE 0 9 WITHSCORES` |
| "Top rated stations in MG" | `rank:postos:rating` + hydrate + UF filter | `ZREVRANGE 0 99 WITHSCORES` |
| "Shell stations in SP active with name containing 'centro'" | RediSearch `idx:postos` | `FT.SEARCH "@bandeira:{Shell} @uf:{SP} @ativo:[1 1] centro"` |
| "Last 7 days of search volume aggregated daily" | TimeSeries `ts:buscas:total` | `TS.RANGE - + AGGREGATION SUM 86400000` |
| "Pipeline health in real time" | Hash `pipeline:metrics` | `HGETALL pipeline:metrics` |

---

## 6. Modeling decisions and trade-offs

### 6.1 Why fuel-specific search rankings?

The alternative would be to keep only the global `rank:buscas:bairro` and filter by fuel at query time. We did not pick that because:

- Query-time filtering would require fetching all documents, bringing them into consumer memory and filtering — violating the principle of a serving layer with <10ms latency.
- Keeping 6 extra sorted sets (1 per fuel) costs ~6× more space, but the cost is amortized: each ZINCRBY takes microseconds.

**Trade-off**: 6× more writes per search event in exchange for reads without post-processing.

### 6.2 Why store `sum` and `count` separately instead of just `media`?

If we stored only the average, incremental updates would require read-modify-write:
```
new_avg = (current_avg * current_count + new_score) / (current_count + 1)
```
That introduces a race condition between multiple parallel consumers.

By keeping `sum` and `count`, every op is atomic:
```
HINCRBYFLOAT posto:{id}:rating sum score
HINCRBY      posto:{id}:rating count 1
```
The average is recomputed and written to the Sorted Set at the end. If two threads process ratings for the same station in parallel, the final result remains correct.

**Trade-off**: two fields in the Hash instead of one, but atomicity guaranteed.

### 6.3 Why 30-day retention on TimeSeries?

- TimeSeries is the Redis structure that grows the most.
- Analytics typically look at the current week or month — rarely beyond 30 days.
- For long-term history MongoDB remains the source of truth (`eventos_preco` is immutable and grows indefinitely there).

**Trade-off**: analytics older than 30 days must be done in Mongo, not in the real-time dashboard.

### 6.4 Why is doc:posto:{id} separated from posto:{id}?

The two Hashes have nearly identical content. The duplication exists because RediSearch indexes hashes by prefix (`PREFIX 1 doc:posto:`), and we wanted the "indexable" hashes to be distinguishable from the "operational" ones (maintenance, debugging, potential future migrations).

**Trade-off**: ~2× memory on station hashes in exchange for clear separation of responsibilities.

### 6.5 Why ObjectId in Redis keys instead of CNPJ?

CNPJ is unique and readable, but:
- Stations may change their legal name while keeping the physical site.
- Multiple stations may share the corporate CNPJ.
- The source-of-truth relationship is `posto._id`, not `posto.cnpj`.

**Trade-off**: less "human-friendly" Redis keys for manual inspection, but referential integrity preserved.

---

## 7. Volume and capacity

With 500k documents in MongoDB distributed across 5 collections of 100k each, Redis materializes approximately:

| Key type | Estimated cardinality | Average size | Total |
|---|---|---|---|
| `posto:{id}` | 100k | ~250 bytes | ~25 MB |
| `posto:{id}:precos` | ~100k | ~100 bytes | ~10 MB |
| `posto:{id}:rating` | ~50k (stations with ratings) | ~80 bytes | ~4 MB |
| `doc:posto:{id}` | 100k | ~250 bytes | ~25 MB |
| `geo:postos[:{fuel}]` | 7 keys | varies with cardinality | ~50 MB |
| `rank:preco:*` | ~500 keys | up to 100k members | ~80 MB |
| `rank:buscas:*` | ~50 keys | ~10k members | ~5 MB |
| `rank:postos:*` | 4 keys | up to 100k members | ~15 MB |
| `ts:*` | ~50k series | 30-day retention | ~100 MB |
| `idx:postos` | 1 index | 100k docs | ~30 MB |
| **Estimated total** | | | **~350 MB** |

The `maxmemory 1.5GB` setting in docker-compose leaves ~4× headroom for growth and page cache, with `allkeys-lru` policy for graceful eviction under pressure.

---

## 8. Suggested future evolution

- **UF sharding on search rankings**: if neighborhood cardinality explodes, split into `rank:buscas:bairro:SP`, `rank:buscas:bairro:RJ`, etc.
- **RedisJSON** for `posto:{id}:rating` with a CAS expression on the average, removing the recompute step outside the pipeline.
- **Stream consumer groups** (XADD/XREADGROUP) to distribute processing across multiple parallel consumers.
- **Bitmap** for hourly opening presence of each station (24×7 = 168 bits per station) — basis for "is this station typically open now?".
- **HyperLogLog** to estimate unique users per UF/city without storing all UUIDs.
