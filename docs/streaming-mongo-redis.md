# Pipeline Streaming MongoDB → Redis (Radar Combustivel)

Este documento descreve em detalhes o fluxo do pipeline construído para o caso **Radar Combustivel** e as decisões tomadas para suportar uma base com **100 mil documentos** distribuídos em cinco coleções (10× maior do que o lab original do Marketplace de Restaurantes).

---

## 1. Coleções de origem

| Coleção | Papel no domínio | Volume típico |
|---|---|---|
| `postos` | Cadastro mestre dos postos (CNPJ, bandeira, endereço, location) | ~25 mil |
| `localizacoes_postos` | Endereço normalizado + bairro + IBGE + GeoJSON Point | ~25 mil |
| `eventos_preco` | Histórico imutável de mudanças de preço por combustível | ~25 mil |
| `buscas_usuarios` | Telemetria de cada busca (raio, filtros, latência, geo do centro) | ~15 mil |
| `avaliacoes_interacoes` | Notas, comentários, check-ins, compartilhamentos, util_count | ~10 mil |

> Os números acima somam cerca de 100 mil documentos. O pipeline foi tunado para esse volume, mas escala linearmente: `BACKFILL_BATCH_SIZE` × `BACKFILL_WORKERS` controla o throughput.

---

## 2. Visão de fluxo

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

## 3. Backfill em lote

O backfill é feito por `pipeline/mongodb_consumer.py:run_backfill`. As principais decisões:

1. **Ordem de processamento.** As coleções `postos` e `localizacoes_postos` são processadas primeiro porque outras coleções (especialmente `eventos_preco`) dependem dos campos de cidade/UF/coordenadas que ficam no Hash `posto:{id}` para construir os rankings.
2. **Batches de 2.000 documentos.** Mantemos cursores no Mongo abertos com `batch_size(2000)` e empilhamos os documentos em uma fila local. Quando a fila atinge `BACKFILL_BATCH_SIZE`, o batch é submetido ao `ThreadPoolExecutor`.
3. **4 workers paralelos.** Cada worker abre sua própria conexão Redis e usa `redis.pipeline(transaction=False)` para enviar várias gravações em uma única ida-e-volta.
4. **Throttling.** Se o número de futuros pendentes ultrapassa `BACKFILL_WORKERS × 2`, paramos de submeter novos batches até drená-los — evita pressão de memória durante o load.
5. **Idempotência.** Como `apply_event` usa `HSET`, `ZADD`, `GEOADD` e `TS.ADD ON_DUPLICATE LAST`, rodar o backfill duas vezes produz o mesmo estado final.

Throughput típico em laptop: **5.000–6.000 docs/s** por coleção, totalizando ~25 segundos para 100k docs.

---

## 4. Streaming via Change Stream

`run_change_streams` abre **5 threads** (uma por coleção) que executam `col.watch` com pipeline `[{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]` e `full_document="updateLookup"`. O documento completo já chega no evento, então o transformer não precisa de nova consulta.

Erros e desconexões são tratados com **backoff exponencial** (1s, 2s, 4s … até 30s). O loop é interrompido apenas pelo signal handler (SIGINT/SIGTERM).

> Por exigir oplog, o MongoDB precisa estar em **replica set**. O serviço `mongo-init` do `docker-compose.yml` faz `rs.initiate(...)` automaticamente.

---

## 5. Mapeamento Mongo → Redis

### 5.1 `postos`
- `HSET posto:{id}` cadastro completo (cnpj, bandeira, endereço…)
- `HSET doc:posto:{id}` espelho indexado pelo RediSearch (`idx:postos`)
- `GEOADD geo:postos lon lat {id}`

### 5.2 `localizacoes_postos`
- Atualiza `posto:{id}` com `bairro`, `municipio`, `uf`, `codigo_ibge`, `lat`, `lon`
- Atualiza `doc:posto:{id}` (índice de busca)
- Atualiza `geo:postos`

### 5.3 `eventos_preco`
- `HSET posto:{id}:precos {fuel} preco_novo` + `… {fuel}:updated_at ts`
- `ZADD rank:preco:{fuel}:{UF} preco {posto}`
- `ZADD rank:preco:{fuel}:cidade:{cidade} preco {posto}`
- `ZADD rank:preco:{fuel}:global preco {posto}`
- `ZADD rank:variacao:{fuel}:24h variacao_pct {posto}`
- `GEOADD geo:postos:{fuel} lon lat {posto}` (apenas postos que vendem o combustível)
- `TS.ADD ts:posto:{id}:{fuel} ts preco`
- `TS.ADD ts:avg:{fuel}:{UF} ts preco` (servirá para média via `TS.RANGE … AGGREGATION AVG`)

### 5.4 `buscas_usuarios`
- `ZINCRBY rank:buscas:bairro 1 "{UF}|{cidade}|{bairro}"`
- `ZINCRBY rank:buscas:cidade 1 "{UF}|{cidade}"`
- `ZINCRBY rank:buscas:uf 1 "{UF}"`
- `ZINCRBY rank:buscas:combustivel 1 "{combustivel}"`
- `TS.INCRBY ts:buscas:total 1 TIMESTAMP {ts}`
- `TS.ADD ts:buscas:latencia {ts} {latencia_ms}`

### 5.5 `avaliacoes_interacoes`
- `HINCRBYFLOAT posto:{id}:rating sum nota` + `HINCRBY count 1`
- Recalcula `media = sum / count` e faz `ZADD rank:postos:rating media {posto}`
- `ZINCRBY rank:postos:util util_count {posto}` (somente quando há `util_count > 0`)
- `ZINCRBY rank:postos:checkin 1 {posto}` quando `tipo == "check_in"`
- `ZINCRBY rank:postos:compartilhamento 1 {posto}` quando `tipo == "compartilhamento"`

---

## 6. Por que cada estrutura?

| Pergunta de negócio | Estrutura escolhida | Motivo |
|---|---|---|
| Posto mais barato | Sorted Set (`rank:preco:…`) | `ZRANGE 0 N` é O(log N + N) |
| Maior alta/queda 24h | Sorted Set por variação | Mantém valor mais recente; `ZREVRANGE` para alta, `ZRANGE` para queda |
| Postos próximos | GEO (`geo:postos`) | `GEOSEARCH BYRADIUS` nativo |
| Postos próximos com fuel X | GEO por fuel | Filtra apenas quem vende o combustível, evita pós-filtro caro |
| Histórico de preço | TimeSeries | Agregação `AVG`/`SUM` por janela já no Redis |
| Volume e latência das buscas | TimeSeries | Operacional / observabilidade |
| Top bairros / cidades / fuel | Sorted Set | `ZINCRBY` atômico |
| Avaliação média | Hash (sum/count) + Sorted Set (média) | Permite atualizar incrementalmente sem recomputar tudo |
| Cadastro / hidratação | Hash (`posto:{id}`) | Lookup O(1) para enriquecer rankings na UI |
| Busca por nome / bandeira / cidade | RediSearch (`idx:postos`) | Full-text + filtros TAG/NUM/GEO em uma única chamada |

---

## 7. Observabilidade

`mongodb_consumer.py` mantém contadores em memória (`Counters`) e a cada `METRICS_INTERVAL_SECONDS` faz `HSET pipeline:metrics`. O dashboard lê esse hash para mostrar:

- Total de eventos processados
- Erros desde o início do processo
- Última coleção atualizada e timestamp
- Eventos por coleção (gráfico de barras)
- Volume de buscas (`ts:buscas:total`)
- Latência média (`ts:buscas:latencia`)

---

## 8. Tratamento de falhas

- Reconexão exponencial em todos os watchers (`backoff = min(backoff*2, 30)`).
- `apply_event` engloba erros por documento e contabiliza `errors`, sem derrubar o pipeline.
- `TS.ADD … ON_DUPLICATE LAST` evita falhas em replay.
- O backfill aceita re-execução: as estruturas Redis são determinísticas para o mesmo input.

---

## 9. Comparação batch × streaming

| Aspecto | Backfill (batch) | Change Stream |
|---|---|---|
| Latência | Segundos a minutos | Milissegundos (operações Mongo → Redis ≤ 50ms no laptop) |
| Custo computacional | Alto e curto | Baixo e contínuo |
| Quando usar | Inicialização ou re-hidratação | Atualizações contínuas |
| Implementação | `find()` + ThreadPoolExecutor | `col.watch(...)` + thread por coleção |

A solução combina os dois: **um backfill no boot** garante o estado correto para os 100k documentos pré-existentes, e **Change Streams** mantêm o Redis sincronizado com qualquer novo `insert/update/replace`.

---

## 10. Próximos passos sugeridos

- Sharding por UF/cidade nas chaves de ranking se a cardinalidade explodir.
- Substituir o cálculo da média via Hash por **RedisJSON + JSON.SET** com expressão CAS.
- Publicar métricas em **Prometheus** e adicionar dashboard Grafana.
- Filtros adicionais no RediSearch (raio dinâmico, faixa de preço, ordenação por nota).
