# Modelagem do Radar Combustível — MongoDB e Redis

> Documento complementar a [`streaming-mongo-redis.md`](streaming-mongo-redis.md).
> Enquanto aquele descreve *como* os dados fluem (pipeline, Change Stream, observabilidade),
> este documento descreve *o que* os dados são (entidades, schemas, chaves, padrões de acesso)
> e *por que* cada decisão de modelagem foi tomada.

---

## 1. Contexto de domínio

A Plataforma Radar Combustível tem cinco entidades principais. Todas têm
`_id` ObjectId nativo do MongoDB e se relacionam por referência (não há
embedding):

```
┌──────────┐                        ┌──────────────────────────┐
│ postos   │───────1:1──────────────│ localizacoes_postos       │
│          │                        │                          │
│          │───────1:N──────────────│ eventos_preco             │
│          │                        │                          │
│          │───────1:N──────────────│ avaliacoes_interacoes     │
└──────────┘                        └──────────────────────────┘
       ▲                                          ▲
       │                                          │
       │ filtro de localização                    │ filtro de posto
       │                                          │
┌──────┴──────────────────────────────────────────┴──────┐
│              buscas_usuarios                            │
│  (referência ao geo_centro, cidade, estado, combustivel)│
└─────────────────────────────────────────────────────────┘
```

**Por que não embedar?** Cada coleção tem cardinalidade e perfil de
acesso diferentes:

- `postos` é cadastro estático (escrita rara, leitura frequente).
- `eventos_preco` cresce continuamente (escrita constante, leitura por
  janelas temporais).
- `buscas_usuarios` cresce continuamente também, mas é desnormalizada
  (já carrega cidade/estado para evitar lookup).

Embedar eventos_preco dentro de postos faria os documentos crescerem
indefinidamente e quebraria a otimização do oplog para Change Streams.

---

## 2. Schemas no MongoDB

### 2.1 `postos`

Cadastro mestre. Um documento por posto, atualizado raramente (cadastro,
mudança de bandeira, fechamento).

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

**Índices**:
- `2dsphere` em `location` (consultas geoespaciais no Mongo, antes da
  serving layer estar disponível).

**Volume**: ~100 mil documentos.

### 2.2 `localizacoes_postos`

Espelho normalizado da localização, com código IBGE oficial. Existe
separadamente porque pode ser atualizada por integrações geográficas
(IBGE, ANP) sem mexer no cadastro.

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

**Índices**:
- `2dsphere` em `geo`.

**Volume**: ~100 mil documentos (1:1 com postos).

### 2.3 `eventos_preco`

Histórico imutável de mudanças de preço. Cada documento é um evento
discreto, nunca atualizado — apenas inserido.

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

**Índices**:
- composto `(posto_id, ocorrido_em DESC)` para histórico por posto.

**Volume**: ~100 mil documentos.

### 2.4 `buscas_usuarios`

Telemetria. Cada documento representa uma consulta feita por um usuário
(app mobile ou web). Inclui geolocalização do centro da busca, filtros e
métricas operacionais (latência, resultado_count).

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

**Índices**:
- composto `(estado, cidade)` para agregação por região.

**Volume**: ~100 mil documentos.

### 2.5 `avaliacoes_interacoes`

Interações sociais com os postos: avaliações com nota, comentários,
check-ins, compartilhamentos. O campo `util_count` é incrementado por
outros usuários ao marcarem aquela interação como útil.

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

**Índices**:
- simples em `posto_id`.

**Volume**: ~100 mil documentos.

---

## 3. Modelo de Serving no Redis

A modelagem do Redis foi **orientada à consulta** (query-driven), não
à entidade. Cada padrão de acesso do dashboard determina uma estrutura
otimizada para aquele padrão. A regra geral: **uma estrutura por pergunta**.

### 3.1 Convenções de chave

Todas as chaves seguem o padrão `<categoria>:<entidade>[:<dimensao>]`,
construídas em código pelo módulo `pipeline/config.RedisKeys` para
evitar duplicação e tipo. As dimensões são separadas por `:` quando
hierárquicas, e por `|` quando compostas dentro de um membro de Sorted
Set.

| Padrão | Significado |
|---|---|
| `posto:{id}` | Hash do cadastro de um posto |
| `geo:postos[:{fuel}]` | Geo index global ou por combustível |
| `rank:<atributo>:<dimensao>` | Sorted Set para ranking ordenado |
| `ts:<entidade>:<dimensao>` | TimeSeries com retenção configurada |
| `idx:postos` | Único índice RediSearch (FT.CREATE) |
| `doc:posto:{id}` | Hash espelho indexado pelo RediSearch |
| `pipeline:metrics` | Hash de telemetria do consumer |

UFs sempre em maiúscula (`SP`, `MG`). Combustíveis sempre em minúscula
nas chaves (`gasolina_comum`), mas em maiúscula nos valores e ranking
de fuel.

### 3.2 Hashes

```
posto:{id}
  posto_id        ObjectId-string (espelhado para indexacao)
  cnpj            string
  nome_fantasia   string
  bandeira        string
  bairro, cidade, estado, uf, cep, logradouro, numero, telefone
  ativo           "0" | "1"
  lat, lon        float-string
  codigo_ibge     string
  updated_at      epoch_ms-string

posto:{id}:precos
  GASOLINA_COMUM             float-string (preço corrente)
  GASOLINA_COMUM:updated_at  epoch_ms-string
  ETANOL                     float-string
  ETANOL:updated_at          epoch_ms-string
  ...

posto:{id}:rating
  sum       float-string (soma das notas)
  count     int-string (quantidade de avaliações)
  util      int-string (soma de util_count)
  checkin   int-string (quantidade de check-ins)
  compart   int-string (quantidade de compartilhamentos)

doc:posto:{id}
  Mesma estrutura de posto:{id}, mas indexada pelo idx:postos.
```

**Por quê Hash?** Acesso O(1) por campo e por chave, permitindo
hidratar tabelas e mapas a partir de IDs vindos de rankings sem segunda
ida ao MongoDB.

### 3.3 Sorted Sets

```
# Preço — score = preço corrente em R$
rank:preco:{fuel}:global              {member: posto_id, score: preco}
rank:preco:{fuel}:{UF}                {member: posto_id, score: preco}
rank:preco:{fuel}:cidade:{cidade}     {member: posto_id, score: preco}

# Variação 24h — score = variacao_pct mais recente
rank:variacao:{fuel}:24h              {member: posto_id, score: variacao_pct}

# Buscas globais — score = total de buscas
rank:buscas:bairro                    {member: "UF|cidade|bairro", score: count}
rank:buscas:cidade                    {member: "UF|cidade", score: count}
rank:buscas:uf                        {member: "UF", score: count}
rank:buscas:combustivel               {member: combustivel, score: count}

# Buscas multi-dimensionais — combustivel-especificas
rank:buscas:bairro:{fuel}             {member: "UF|cidade|bairro", score: count}
rank:buscas:cidade:{fuel}             {member: "UF|cidade", score: count}

# Combustíveis mais buscados por região
rank:buscas:fuel:{UF}                 {member: combustivel, score: count}
rank:buscas:fuel:{UF}|{cidade}        {member: combustivel, score: count}

# Engajamento dos postos
rank:postos:rating                    {member: posto_id, score: media}
rank:postos:util                      {member: posto_id, score: soma_util_count}
rank:postos:checkin                   {member: posto_id, score: total_check_ins}
rank:postos:compartilhamento          {member: posto_id, score: total_compart}
```

**Por quê Sorted Set?** `ZRANGE` em O(log N + k) para top-N por score,
`ZINCRBY` atômico para contadores, e `ZADD` permite atualizar score
mantendo a posição correta automaticamente.

### 3.4 Geo

```
geo:postos                    Geo de todos os postos (qualquer combustível)
geo:postos:gasolina_comum     Geo só dos postos que vendem gasolina comum
geo:postos:etanol             Geo só dos postos que vendem etanol
...
```

**Por quê uma chave por combustível?** Evita pós-filtragem. A consulta
"postos perto de mim que vendem diesel S10" vira um único
`GEOSEARCH geo:postos:diesel_s10 ...` em vez de buscar todos e filtrar
em memória.

### 3.5 TimeSeries

```
ts:posto:{id}:{fuel}          Histórico de preço de um posto/combustível
  Labels: posto={id}, combustivel={fuel}, uf={UF}
  Retention: 30 dias
  DUPLICATE_POLICY: LAST

ts:avg:{fuel}:{UF}            Preços brutos por UF (para agregação AVG via TS.RANGE)
  Labels: combustivel={fuel}, uf={UF}, kind=raw

ts:buscas:total               Contador incremental do volume de buscas
  Labels: kind=buscas_total

ts:buscas:latencia            Latência de cada busca individual
  Labels: kind=buscas_latencia_ms
```

**Por quê TimeSeries?** Agregação por janela nativa do Redis
(`TS.RANGE ... AGGREGATION AVG 60000`). Sem TimeSeries, teríamos que
manter listas e calcular médias em memória no consumer.

### 3.6 RediSearch — o único índice formal

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

**Por quê RediSearch?** Único caminho para combinar full-text com
filtros TAG, numéricos e geo em uma única chamada com baixa latência.
Suporta sintaxe expressiva como
`@cuisine:{pizza} @bairro:{Pinheiros} @stars:[4.5 5]`.

---

## 4. Mapeamento Mongo → Redis por evento

Cada inserção/atualização de documento no Mongo dispara um conjunto
específico de comandos Redis. Tudo idempotente.

### 4.1 Insert em `postos`
```
HSET posto:{id}      cnpj nome_fantasia bandeira ... lat lon
HSET doc:posto:{id}  <espelho>                             (RediSearch indexa)
GEOADD geo:postos    lon lat {id}
```

### 4.2 Insert em `localizacoes_postos`
```
HSET posto:{id}      municipio bairro uf codigo_ibge lat lon
HSET doc:posto:{id}  <mesmos campos>
GEOADD geo:postos    lon lat {id}
```

### 4.3 Insert em `eventos_preco`
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

### 4.4 Insert em `buscas_usuarios`
```
ZINCRBY rank:buscas:bairro                    1 "UF|cidade|bairro"
ZINCRBY rank:buscas:cidade                    1 "UF|cidade"
ZINCRBY rank:buscas:uf                        1 "UF"
ZINCRBY rank:buscas:combustivel               1 combustivel

ZINCRBY rank:buscas:bairro:{fuel}             1 "UF|cidade|bairro"
ZINCRBY rank:buscas:cidade:{fuel}             1 "UF|cidade"
ZINCRBY rank:buscas:fuel:{UF}                 1 combustivel
ZINCRBY rank:buscas:fuel:{UF}|{cidade}        1 combustivel

TS.INCRBY ts:buscas:total                     1     TIMESTAMP ts_ms
TS.ADD    ts:buscas:latencia                  ts_ms latencia_ms
```

### 4.5 Insert em `avaliacoes_interacoes`
```
# se tem nota:
HINCRBYFLOAT posto:{id}:rating  sum   nota
HINCRBY      posto:{id}:rating  count 1
ZADD         rank:postos:rating media {id}    (recalcula = sum/count)

# se util_count > 0:
HINCRBY      posto:{id}:rating  util  util_count
ZINCRBY      rank:postos:util   util_count {id}

# se tipo == check_in:
HINCRBY      posto:{id}:rating       checkin 1
ZINCRBY      rank:postos:checkin     1 {id}

# se tipo == compartilhamento:
HINCRBY      posto:{id}:rating              compart 1
ZINCRBY      rank:postos:compartilhamento   1 {id}
```

---

## 5. Padrões de acesso × estrutura escolhida

| Pergunta do usuário | Estrutura usada | Comando-tipo |
|---|---|---|
| "Onde está o posto mais barato de gasolina aqui em SP?" | Sorted Set `rank:preco:gasolina_comum:SP` | `ZRANGE 0 9 WITHSCORES` |
| "Qual o menor preço de etanol em Itajubá?" | Sorted Set `rank:preco:etanol:cidade:Itajuba` | `ZRANGE 0 0 WITHSCORES` |
| "Quem subiu mais o diesel S10 nas últimas 24h?" | Sorted Set `rank:variacao:diesel_s10:24h` | `ZREVRANGE 0 9 WITHSCORES` |
| "Postos de etanol num raio de 5 km daqui" | Geo `geo:postos:etanol` | `GEOSEARCH FROMLONLAT lon lat BYRADIUS 5 km` |
| "Como o preço da gasolina do posto X variou no mês?" | TimeSeries `ts:posto:{id}:gasolina_comum` | `TS.RANGE - + AGGREGATION AVG 3600000` |
| "Quais bairros de MG mais buscam diesel?" | Sorted Set `rank:buscas:bairro:diesel_s10` + filtro prefixo `MG|` | `ZREVRANGE 0 999` + filter |
| "Quais combustíveis mais buscam em Itajubá?" | Sorted Set `rank:buscas:fuel:MG|Itajuba` | `ZREVRANGE 0 9 WITHSCORES` |
| "Top postos avaliados em MG" | `rank:postos:rating` + hydrate + filtro UF | `ZREVRANGE 0 99 WITHSCORES` |
| "Buscar postos da Shell em SP ativos com nome contendo 'centro'" | RediSearch `idx:postos` | `FT.SEARCH "@bandeira:{Shell} @uf:{SP} @ativo:[1 1] centro"` |
| "Volume de buscas dos últimos 7 dias agregado por dia" | TimeSeries `ts:buscas:total` | `TS.RANGE - + AGGREGATION SUM 86400000` |
| "Saúde do pipeline em tempo real" | Hash `pipeline:metrics` | `HGETALL pipeline:metrics` |

---

## 6. Decisões de modelagem e seus trade-offs

### 6.1 Por que rankings combustível-específicos para buscas?

A alternativa seria manter apenas `rank:buscas:bairro` global e
filtrar por combustível no momento da consulta. Não escolhemos isso
porque:

- Filtrar em tempo de consulta exigiria buscar todos os documentos,
  trazer pra memória do consumer e filtrar — viola o princípio de
  serving layer com latência <10ms.
- Manter 6 sorted sets adicionais (1 por combustível) custa ~6× mais
  espaço, mas o custo é amortizado: cada ZINCRBY toma microssegundos.

**Trade-off**: 6× mais escritas por evento de busca em troca de leituras
sem pós-processamento.

### 6.2 Por que armazenar `sum` e `count` separadamente em vez de só `media`?

Se guardássemos só a média, atualizar incrementalmente exigiria
ler-modificar-escrever:
```
media_nova = (media_atual * count_atual + nova_nota) / (count_atual + 1)
```
Isso introduz race condition entre múltiplos consumers em paralelo.

Mantendo `sum` e `count`, cada operação é atômica:
```
HINCRBYFLOAT posto:{id}:rating sum nota
HINCRBY      posto:{id}:rating count 1
```
A média é recalculada e gravada no Sorted Set no fim. Se duas threads
processarem avaliações para o mesmo posto em paralelo, o resultado
final continua correto.

**Trade-off**: dois campos no Hash em vez de um, mas atomicidade
garantida.

### 6.3 Por que retenção de 30 dias em TimeSeries?

- TimeSeries é a estrutura que mais cresce no Redis.
- Análises tipicamente olham para semana corrente ou mês — raramente
  além de 30 dias.
- Para histórico longo, o MongoDB continua sendo a fonte da verdade
  (eventos_preco é imutável e cresce indefinidamente lá).

**Trade-off**: análises > 30 dias precisam ser feitas no Mongo, não no
dashboard tempo real.

### 6.4 Por que doc:posto:{id} é separado de posto:{id}?

Os dois Hashes têm conteúdo praticamente idêntico. A duplicação existe
porque o RediSearch indexa hashes por prefixo (`PREFIX 1 doc:posto:`),
e queríamos que os hashes "indexáveis" fossem distinguíveis dos
"operacionais" (manutenção, debug, possíveis migrações futuras).

**Trade-off**: ~2× memória nos hashes de posto em troca de separação
clara de responsabilidades.

### 6.5 Por que ObjectId nas chaves do Redis em vez de CNPJ?

CNPJ é único e legível, mas:
- Postos podem mudar de razão social mantendo o ponto físico.
- Múltiplos postos podem compartilhar CNPJ corporativo.
- A relação fonte da verdade é `posto._id`, não `posto.cnpj`.

**Trade-off**: chaves Redis menos "amigáveis" para inspeção manual,
mas integridade referencial preservada.

---

## 7. Volume e capacidade

Com 500 mil documentos no MongoDB distribuídos em 5 coleções de 100k cada,
o Redis materializa aproximadamente:

| Tipo de chave | Cardinalidade estimada | Tamanho médio | Total |
|---|---|---|---|
| `posto:{id}` | 100k | ~250 bytes | ~25 MB |
| `posto:{id}:precos` | ~100k | ~100 bytes | ~10 MB |
| `posto:{id}:rating` | ~50k (postos com avaliações) | ~80 bytes | ~4 MB |
| `doc:posto:{id}` | 100k | ~250 bytes | ~25 MB |
| `geo:postos[:{fuel}]` | 7 chaves | varia com cardinalidade | ~50 MB |
| `rank:preco:*` | ~500 chaves | 100k membros máx. | ~80 MB |
| `rank:buscas:*` | ~50 chaves | ~10k membros | ~5 MB |
| `rank:postos:*` | 4 chaves | até 100k membros | ~15 MB |
| `ts:*` | ~50k séries | retention 30 dias | ~100 MB |
| `idx:postos` | 1 índice | 100k docs | ~30 MB |
| **Total estimado** | | | **~350 MB** |

A configuração `maxmemory 1.5GB` no docker-compose deixa ~4× de
margem para crescimento e cache de páginas, com policy `allkeys-lru`
para descarte gracioso em caso de pressão.

---

## 8. Evolução futura sugerida

- **Sharding por UF nos rankings de buscas**: se a cardinalidade dos
  bairros explodir, separar `rank:buscas:bairro:SP`,
  `rank:buscas:bairro:RJ`, etc.
- **RedisJSON** para `posto:{id}:rating` com expressão CAS na média,
  eliminando o passo de recalcular fora do pipeline.
- **Stream consumer groups** (XADD/XREADGROUP) para distribuir o
  processamento entre múltiplos consumers paralelos.
- **Bitmap** para presença horária de cada posto (24×7 = 168 bits por
  posto) — base para "este posto costuma estar aberto agora?".
- **HyperLogLog** para estimar usuários únicos por UF/cidade sem
  guardar todos os UUIDs.
