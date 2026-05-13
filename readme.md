# Radar Combustivel — Pipeline Streaming MongoDB → Redis

> **MBA FIAP em Tecnologia | Banco de Dados In-Memory**
> Caso de uso: **Plataforma Radar Combustivel** (preços, postos, buscas e avaliações)
> Baseado no laboratório `commithouse/lab-streaming-mongo-redis` (Marketplace de Restaurantes), adaptado para 100 mil documentos e cinco coleções de domínio.

---

## 1. Visão geral

A plataforma **Radar Combustivel** monitora postos de combustível, preços, localização, buscas de usuários e interações (check-in, compartilhamento, avaliação). Esta solução transforma esses dados, armazenados no **MongoDB**, em uma **camada de _serving_ no Redis** capaz de responder em poucos milissegundos a perguntas como:

- Quais postos estão com **menor preço** por região / cidade?
- Quais combustíveis estão **em alta** (variação positiva nas últimas 24h)?
- Quais bairros apresentam **maior volume de buscas**?
- Quais postos tiveram **maior variação recente de preço**?
- Quais postos estão **mais próximos** do usuário e vendendo um combustível específico?
- Quais postos têm **melhor avaliação** e mais check-ins?

Para isso, um **pipeline Python** lê os documentos do MongoDB, transforma-os em eventos e atualiza estruturas no Redis (Hashes, Sorted Sets, Geo, TimeSeries e RediSearch). Um dashboard **Streamlit** consome apenas o Redis para entregar visualizações em tempo (quase) real.

---

## 2. Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                       MongoDB 7                             │
│  radar_combustivel  (replica set rs0 — 100k docs)           │
│  ├─ postos                                                  │
│  ├─ localizacoes_postos                                     │
│  ├─ eventos_preco                                           │
│  ├─ buscas_usuarios                                         │
│  └─ avaliacoes_interacoes                                   │
└──────────────────────────┬──────────────────────────────────┘
        backfill (find batch) │       Change Stream (col.watch)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        Pipeline Python  ─  pipeline/mongodb_consumer.py     │
│  - Backfill paralelo em batches de 2.000 docs               │
│  - 5 threads de Change Stream (uma por coleção)             │
│  - Reconexão exponencial + métricas em pipeline:metrics     │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Redis Stack 7.4 (serving)                  │
│  Hash    posto:{id}                — cadastro resumido      │
│  Hash    posto:{id}:precos         — preço corrente / fuel  │
│  Hash    posto:{id}:rating         — sum/count/util/checkin │
│  GEO     geo:postos                — localização global     │
│  GEO     geo:postos:{combustivel}  — só quem vende o fuel   │
│  ZSET    rank:preco:{fuel}:{UF}    — menor preço por UF     │
│  ZSET    rank:preco:{fuel}:cidade  — menor preço por cidade │
│  ZSET    rank:preco:{fuel}:global  — menor preço global     │
│  ZSET    rank:variacao:{fuel}:24h  — maior alta/queda 24h   │
│  ZSET    rank:buscas:{bairro|cid…} — top buscas             │
│  ZSET    rank:postos:{rating|util} — engajamento            │
│  TS      ts:posto:{id}:{fuel}      — histórico de preço     │
│  TS      ts:avg:{fuel}:{UF}        — média por UF           │
│  TS      ts:buscas:total / lat.    — volume e latência      │
│  IDX     idx:postos (RediSearch)   — full-text + tag + geo  │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│   Dashboard Streamlit  ─  queries/data-view.py              │
│   • Visão geral / saúde do pipeline                         │
│   • Preços, rankings, variação 24h e séries temporais       │
│   • Mapa GEOSEARCH (raio configurável)                      │
│   • Comportamento (top bairros / cidades / combustíveis)    │
│   • Avaliações e interações                                 │
└─────────────────────────────────────────────────────────────┘
```

> Detalhes adicionais do pipeline em [`docs/streaming-mongo-redis.md`](docs/streaming-mongo-redis.md).

---

## 3. Estrutura do repositório

```
radar-combustivel/
├── docker-compose.yml          # MongoDB rs0 + Redis Stack
├── requirements.txt            # Dependências Python
├── .env.example                # Template de variáveis
├── readme.md                   # (este arquivo)
├── docs/
│   ├── streaming-mongo-redis.md
│   └── arquitetura.svg
├── init/
│   └── redis_indexes.py        # Cria indices RediSearch
├── pipeline/
│   ├── config.py               # Variáveis e catálogo de chaves Redis
│   ├── event_transformer.py    # Mongo doc → comandos Redis
│   └── mongodb_consumer.py     # Backfill + Change Streams
└── queries/
    ├── redis_reader.py         # Funções de leitura + CLI demo
    └── data-view.py            # Streamlit
```

---

## 4. Configuração do ambiente

### 4.1 Pré-requisitos

- Docker + Docker Compose
- Python 3.10+
- (Opcional) MongoDB Compass e RedisInsight

### 4.2 Variáveis de ambiente

```bash
cp .env.example .env
```

A configuração padrão atende ao docker-compose. Para 100 mil documentos as variáveis-chave são:

| Variável | Default | Por quê |
|---|---|---|
| `BACKFILL_BATCH_SIZE` | `2000` | Baixa pressão sobre Redis e mantém pipeline com 50–80 batches em paralelo |
| `BACKFILL_WORKERS` | `4` | 4 threads escrevendo no Redis em pipeline; benchmark local processa ~6k docs/s |
| `TS_RETENTION_MS` | `30 dias` | Mantém histórico de preço suficiente para análises de tendência |
| `RANKING_WINDOW_HOURS` | `24` | Janela usada para variação % de preços |
| `STREAMLIT_REFRESH_SECONDS` | `10` | Tempo padrão de auto-refresh do dashboard |

### 4.3 Subir a infraestrutura

```bash
docker compose up -d
```

O serviço `mongo-init` inicializa o replica set `rs0` automaticamente (necessário para Change Streams).

> **Importante**: a base `radar_combustivel` deste trabalho **já existe** com 100k documentos nas cinco coleções (conforme imagem de evidência fornecida). Caso o Mongo do compose esteja vazio, basta restaurar o dump usando `mongorestore`.

### 4.4 Instalar dependências Python

```bash
python -m venv .venv
.\.venv\Scripts\activate          # Windows
# ou: source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
```
### 4.5 Rodar o novo seed 

```bash
python init/seed.py --drop 

# popula Mongo (500k docs em 125 cidades e coordenadas reais)
```
### 4.6 Criar índices RediSearch

```bash
python init/redis_indexes.py
```

---

## 5. Executando o pipeline

### Terminal 1 — Consumer (backfill + Change Stream)

```bash
python -m pipeline.mongodb_consumer
```

Saída esperada (resumida):

```
[backfill] postos: 25.000 docs em 4.2s (5950 docs/s)
[backfill] localizacoes_postos: 25.000 docs em 4.0s (6240 docs/s)
[backfill] eventos_preco: 25.000 docs em 7.6s (3290 docs/s)
[backfill] buscas_usuarios: 15.000 docs em 2.5s (5980 docs/s)
[backfill] avaliacoes_interacoes: 10.000 docs em 1.6s (6450 docs/s)
[watch] abrindo Change Stream em postos
[watch] abrindo Change Stream em eventos_preco
metrics: total=100000 erros=0 ultimo=eventos_preco
```

> Use `SKIP_BACKFILL=1` em execuções subsequentes para pular o backfill e ir direto ao streaming.

### Terminal 2 — Consultas demo

```bash
python queries/redis_reader.py
```

### Terminal 3 — Dashboard Streamlit

```bash
streamlit run queries/data-view.py
```

Acesse `http://localhost:8501`. As cinco abas (Visão geral, Preços, Mapa & Geo, Comportamento, Avaliações) consomem **apenas o Redis** e atualizam-se automaticamente.

### Terminal 4 — Pipeline Online (change stream)

```bash
python init/insert_test_avaliacoes.py

# insere novos documentos de avaliações para um determinado posto, pegar o status da tela de avaliações e visão geral antes e rodar algumas vezes, ver o reflexo das alterações nestas telas do dashboard carregado pelo terminal 3 e no mongodb_consumer.py no terminal 1. Esse teste  também pode ser realizado inserindo documentos diretamente no mongodb.
```
---

## 6. Estruturas Redis adotadas

| Estrutura | Chave | Por que escolhemos |
|---|---|---|
| **Hash** | `posto:{id}` | acesso O(1) ao cadastro completo; ideal para hidratar tabelas |
| **Hash** | `posto:{id}:precos` | um campo por combustível, atualização atômica via `HSET` |
| **Sorted Set** | `rank:preco:{fuel}:{UF}` | `ZRANGE 0 N` devolve menor preço em O(log N) |
| **Sorted Set** | `rank:variacao:{fuel}:24h` | mostra altas/quedas com `ZREVRANGE`/`ZRANGE` |
| **Sorted Set** | `rank:buscas:bairro/cidade/combustivel` | `ZINCRBY` mantém o ranking automaticamente |
| **Sorted Set** | `rank:postos:{rating,checkin,util}` | métricas de engajamento ordenadas |
| **GEO** | `geo:postos`, `geo:postos:{fuel}` | `GEOSEARCH BYRADIUS` para buscas de proximidade |
| **TimeSeries** | `ts:posto:{id}:{fuel}` | evolução de preço com agregação nativa por janela |
| **TimeSeries** | `ts:avg:{fuel}:{UF}` | média por UF para gráficos comparativos |
| **TimeSeries** | `ts:buscas:total`, `ts:buscas:latencia` | observabilidade do funil de busca |
| **RediSearch** | `idx:postos` | full-text por nome / cidade / bairro + filtros TAG (UF, bandeira) e geo |

---

## 7. Decisões de arquitetura

| Decisão | Escolha | Justificativa |
|---|---|---|
| Captura de eventos | MongoDB Change Stream com `full_document=updateLookup` | Evita polling e garante o documento completo para o transformer |
| Backfill | Cursor `find()` + `ThreadPoolExecutor` (4 workers, batches 2k) | Carrega 100k docs em poucos segundos sem saturar o Redis |
| Pipeline Redis | `redis-py` `pipeline(transaction=False)` por evento | Reduz latência de RTT em até 5× quando há vários comandos |
| Atomicidade do ranking | `ZADD` (preço) + `ZINCRBY` (buscas) | Operações nativas atômicas em O(log N) |
| Variação 24h | `ZADD` substitui o score anterior | Última variação prevalece — coerente com o significado de “mais recente” |
| TimeSeries | Retenção de 30 dias + `DUPLICATE_POLICY LAST` | Recuperação rápida e idempotência no replay |
| Observabilidade | Hash `pipeline:metrics` flush a cada 15s | Dashboard mostra saúde do pipeline sem tocar no Mongo |
| Reconexão | `tenacity`/loop exponencial até 30s | Pipeline auto-recupera de quedas do Mongo/Redis |
| Cache do Redis | `maxmemory 1.5GB`, `allkeys-lru`, AOF on | Suficiente para 100k postos + indices + 30 dias de TS |

---

## 8. Diferenciais implementados

- Consultas **geográficas** com `GEOSEARCH` por combustível.
- **Séries temporais** de preço por posto e média por UF, com agregação nativa.
- **Ranking** por bairro, cidade, UF e combustível.
- **Backfill paralelo** vs streaming `col.watch` — o mesmo transformer atende às duas modalidades.
- **Observabilidade**: pipeline registra throughput, último evento, erros e tipo da última coleção em `pipeline:metrics`.
- **Tratamento de falhas**: reconexão exponencial nos Change Streams; `apply_event` é idempotente.
- **Multi-visões** no Streamlit com filtros (combustível/UF/cidade), mapa interativo (`pydeck`), gráficos (`plotly`) e auto-refresh.

---

## 9. Comandos úteis

```bash
# limpar tudo
docker compose down -v

# inspecionar Redis (CLI)
docker exec -it radar-redis redis-cli
> HGETALL pipeline:metrics
> ZRANGE rank:preco:gasolina_comum:SP 0 9 WITHSCORES
> GEOSEARCH geo:postos FROMLONLAT -46.6333 -23.5505 BYRADIUS 5 km ASC WITHCOORD COUNT 20
> TS.RANGE ts:buscas:total - + AGGREGATION SUM 3600000

# inspecionar Mongo (CLI)
docker exec -it radar-mongo mongosh
> use radar_combustivel
> db.eventos_preco.countDocuments()
```

---

## 10. Checklist de validação

- [ ] `docker compose up -d` sobe Mongo (rs0) e Redis Stack sem erros.
- [ ] `python init/seed.py --drop`popula Mongo (500k docs em 125 cidades reais). 
- [ ] `python init/redis_indexes.py` cria `idx:postos` sem erro.
- [ ] `python -m pipeline.mongodb_consumer` roda backfill e abre 5 Change Streams.
- [ ] `python queries/redis_reader.py` imprime rankings populados.
- [ ] `streamlit run queries/data-view.py` abre as cinco abas com gráficos.
- [ ] Inserir um documento novo em `eventos_preco` pelo mongoDB reflete no dashboard < 5s e/ou,
- [ ] `python init/insert_test_avaliacoes.py` insere novas avaliações para um determinado posto isso também reflete no dashboard.  

---

## 11. Referências

- MongoDB Change Streams — https://www.mongodb.com/docs/manual/changeStreams/
- Redis Sorted Sets — https://redis.io/docs/data-types/sorted-sets/
- RediSearch — https://redis.io/docs/interact/search-and-query/
- RedisTimeSeries — https://redis.io/docs/data-types/timeseries/
- Lab base (Marketplace de Restaurantes) — https://github.com/commithouse/lab-streaming-mongo-redis
