import os
from typing import Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
    TextField,
)
from redis.commands.search.index_definition import (
    IndexDefinition,
    IndexType,
)

# =========================================================
# ENVIRONMENT
# =========================================================

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/?directConnection=true",
)

REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "localhost",
)

REDIS_PORT = int(
    os.getenv(
        "REDIS_PORT",
        "6379",
    )
)

DB_NAME = "radar_combustivel"

# Mongo collections
COL_POSTOS = "postos"
COL_LOCALIZACOES = "localizacoes_postos"
COL_EVENTOS_PRECO = "eventos_preco"
COL_AVALIACOES = "avaliacoes_interacoes"
COL_BUSCAS = "buscas"

# =========================================================
# LOAD SNAPSHOT
# =========================================================

def load_postos_snapshot() -> Dict[str, dict]:

    mongo = MongoClient(MONGO_URI)

    db = mongo[DB_NAME]

    postos_col = db[COL_POSTOS]
    local_col = db[COL_LOCALIZACOES]
    preco_col = db[COL_EVENTOS_PRECO]
    aval_col = db[COL_AVALIACOES]

    postos = {}

    # =====================================================
    # POSTOS
    # =====================================================

    for posto in postos_col.find():

        posto_id = str(posto["_id"])

        postos[posto_id] = {
            "posto_id": posto_id,
            "nome_fantasia": posto.get(
                "nome_fantasia",
                "",
            ),
            "bandeira": posto.get(
                "bandeira",
                "",
            ),
            "cnpj": posto.get(
                "cnpj",
                "",
            ),
            "cidade": posto.get(
                "endereco",
                {},
            ).get(
                "cidade",
                "",
            ),
            "estado": posto.get(
                "endereco",
                {},
            ).get(
                "estado",
                "",
            ),
            "bairro": posto.get(
                "endereco",
                {},
            ).get(
                "bairro",
                "",
            ),
            "logradouro": posto.get(
                "endereco",
                {},
            ).get(
                "logradouro",
                "",
            ),
            "ativo": int(
                bool(
                    posto.get(
                        "ativo",
                        True,
                    )
                )
            ),
            "telefone": posto.get(
                "telefone",
                "",
            ),
            "location": posto.get(
                "location",
                {},
            ),
            "precos": {},
            "variacoes": {},
            "media_avaliacao": 0.0,
            "total_avaliacoes": 0,
            "shares": 0,
        }

    # =====================================================
    # LOCALIZAÇÃO MAIS RECENTE
    # =====================================================

    pipeline_local = [
        {
            "$sort": {
                "atualizado_em": -1
            }
        },
        {
            "$group": {
                "_id": "$posto_id",
                "geo": {
                    "$first": "$geo"
                },
                "municipio": {
                    "$first": "$municipio"
                },
                "bairro": {
                    "$first": "$bairro"
                },
                "uf": {
                    "$first": "$uf"
                },
            }
        },
    ]

    for row in local_col.aggregate(
        pipeline_local
    ):

        posto_id = str(row["_id"])

        if posto_id not in postos:
            continue

        postos[posto_id]["geo"] = row.get(
            "geo",
            {},
        )

    # =====================================================
    # PREÇOS MAIS RECENTES
    # =====================================================

    pipeline_precos = [
        {
            "$sort": {
                "ocorrido_em": -1
            }
        },
        {
            "$group": {
                "_id": {
                    "posto_id": "$posto_id",
                    "combustivel": "$combustivel",
                },
                "preco_novo": {
                    "$first": "$preco_novo"
                },
                "variacao_pct": {
                    "$first": "$variacao_pct"
                },
                "timestamp": {
                    "$first": "$ocorrido_em"
                },
            }
        },
    ]

    for row in preco_col.aggregate(
        pipeline_precos
    ):

        posto_id = str(
            row["_id"]["posto_id"]
        )

        combustivel = row["_id"][
            "combustivel"
        ]

        if posto_id not in postos:
            continue

        preco = float(
            row.get(
                "preco_novo",
                0,
            )
        )

        variacao = float(
            row.get(
                "variacao_pct",
                0,
            )
        )

        timestamp = row.get(
            "timestamp"
        )

        postos[posto_id]["precos"][
            combustivel
        ] = {
            "preco": preco,
            "timestamp": timestamp,
        }

        postos[posto_id]["variacoes"][
            combustivel
        ] = variacao

    # =====================================================
    # AVALIAÇÕES
    # =====================================================

    pipeline_avaliacoes = [
        {
            "$match": {
                "nota": {
                    "$ne": None
                }
            }
        },
        {
            "$group": {
                "_id": "$posto_id",
                "media_avaliacao": {
                    "$avg": "$nota"
                },
                "total_avaliacoes": {
                    "$sum": 1
                },
            }
        },
    ]

    for row in aval_col.aggregate(
        pipeline_avaliacoes
    ):

        posto_id = str(row["_id"])

        if posto_id not in postos:
            continue

        postos[posto_id][
            "media_avaliacao"
        ] = round(
            float(
                row.get(
                    "media_avaliacao",
                    0,
                )
            ),
            2,
        )

        postos[posto_id][
            "total_avaliacoes"
        ] = int(
            row.get(
                "total_avaliacoes",
                0,
            )
        )

    # =====================================================
    # SHARES
    # =====================================================

    pipeline_shares = [
        {
            "$match": {
                "tipo": "compartilhamento"
            }
        },
        {
            "$group": {
                "_id": "$posto_id",
                "shares": {
                    "$sum": 1
                },
            }
        },
    ]

    for row in aval_col.aggregate(
        pipeline_shares
    ):

        posto_id = str(row["_id"])

        if posto_id not in postos:
            continue

        postos[posto_id]["shares"] = int(
            row.get(
                "shares",
                0,
            )
        )

    return postos


# =========================================================
# BUSCAS
# =========================================================

def load_combustivel_buscas():

    mongo = MongoClient(MONGO_URI)

    db = mongo[DB_NAME]

    buscas_col = db[COL_BUSCAS]

    pipeline = [
        {
            "$group": {
                "_id": "$combustivel",
                "total": {
                    "$sum": 1
                },
            }
        }
    ]

    return list(
        buscas_col.aggregate(pipeline)
    )


def load_bairro_buscas():

    mongo = MongoClient(MONGO_URI)

    db = mongo[DB_NAME]

    buscas_col = db[COL_BUSCAS]

    pipeline = [
        {
            "$group": {
                "_id": "$bairro",
                "total": {
                    "$sum": 1
                },
            }
        }
    ]

    return list(
        buscas_col.aggregate(pipeline)
    )


# =========================================================
# MAIN
# =========================================================

def main() -> None:

    redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    snapshot = load_postos_snapshot()

    # =====================================================
    # LIMPEZA
    # =====================================================

    print(
        "[REDIS] Limpando estruturas antigas..."
    )

    keys = redis.keys(
        "ranking:*"
    )

    for key in keys:
        redis.delete(key)

    # =====================================================
    # HASHES + GEO + RANKINGS + TIMESERIES
    # =====================================================

    for posto_id, item in snapshot.items():

        coords = (
            item.get(
                "location",
                {},
            ).get(
                "coordinates",
                [0, 0],
            )
        )

        lon = coords[0]
        lat = coords[1]

        bairro = item.get(
            "bairro",
            "desconhecido",
        )

        precos = item.get(
            "precos",
            {},
        )

        variacoes = item.get(
            "variacoes",
            {},
        )

        gasolina = precos.get(
            "GASOLINA_COMUM",
            {},
        ).get(
            "preco",
            0,
        )

        etanol = precos.get(
            "ETANOL",
            {},
        ).get(
            "preco",
            0,
        )

        diesel = precos.get(
            "DIESEL_S10",
            {},
        ).get(
            "preco",
            0,
        )

        # =================================================
        # HASH
        # =================================================

        redis.hset(
            f"posto:{posto_id}",
            mapping={

                "posto_id": posto_id,
                "nome_fantasia": item.get(
                    "nome_fantasia",
                    "",
                ),
                "bandeira": item.get(
                    "bandeira",
                    "",
                ),
                "cnpj": item.get(
                    "cnpj",
                    "",
                ),

                "cidade": item.get(
                    "cidade",
                    "",
                ),

                "estado": item.get(
                    "estado",
                    "",
                ),

                "bairro": bairro,

                "logradouro": item.get(
                    "logradouro",
                    "",
                ),

                "ativo": item.get(
                    "ativo",
                    1,
                ),

                "media_avaliacao": item.get(
                    "media_avaliacao",
                    0,
                ),

                "total_avaliacoes": item.get(
                    "total_avaliacoes",
                    0,
                ),

                "shares": item.get(
                    "shares",
                    0,
                ),

                "location": f"{lon},{lat}",

                "gasolina_comum": gasolina,
                "etanol": etanol,
                "diesel_s10": diesel,
            },
        )

        # =================================================
        # GEO
        # =================================================

        try:

            redis.geoadd(
                "postos:geo",
                [
                    lon,
                    lat,
                    posto_id,
                ],
            )

        except Exception:
            pass

        # =================================================
        # RANKING PREÇO
        # =================================================

        rankings_preco = {
            "gasolina_comum": gasolina,
            "etanol": etanol,
            "diesel_s10": diesel,
        }

        for combustivel, preco in rankings_preco.items():

            if preco > 0:

                redis.zadd(
                    f"ranking:preco:{combustivel}:{bairro}",
                    {
                        posto_id: preco
                    },
                )

        # =================================================
        # RANKING VARIAÇÃO
        # =================================================

        for combustivel, delta in variacoes.items():

            combustivel = (
                combustivel.lower()
            )

            redis.zadd(
                f"ranking:variacao:{combustivel}",
                {
                    posto_id: abs(delta)
                },
            )

        # =================================================
        # TIMESERIES
        # =================================================

        combustiveis_ts = {
            "gasolina_comum": gasolina,
            "etanol": etanol,
            "diesel_s10": diesel,
        }

        for combustivel, preco in combustiveis_ts.items():

            ts_key = (
                f"ts:posto:{posto_id}:{combustivel}"
            )

            try:

                redis.execute_command(
                    "TS.CREATE",
                    ts_key,
                    "RETENTION",
                    604800000,
                    "LABELS",
                    "posto_id",
                    posto_id,
                    "combustivel",
                    combustivel,
                )

            except Exception:
                pass

            try:

                redis.execute_command(
                    "TS.ADD",
                    ts_key,
                    "*",
                    preco,
                )

            except Exception:
                pass

    # =====================================================
    # COMBUSTÍVEIS EM ALTA
    # =====================================================

    combustiveis_busca = (
        load_combustivel_buscas()
    )

    for row in combustiveis_busca:

        combustivel = row["_id"]
        total = row["total"]

        redis.zadd(
            "ranking:combustivel:buscas",
            {
                combustivel: total
            },
        )

    # =====================================================
    # BAIRROS MAIS BUSCADOS
    # =====================================================

    bairros_busca = (
        load_bairro_buscas()
    )

    for row in bairros_busca:

        bairro = row["_id"]
        total = row["total"]

        redis.zadd(
            "ranking:buscas:bairro",
            {
                bairro: total
            },
        )

    # =====================================================
    # REDISEARCH
    # =====================================================

    try:

        redis.execute_command(
            "FT.DROPINDEX",
            "idx:postos",
            "DD",
        )

    except Exception:
        pass

    redis.ft(
        "idx:postos"
    ).create_index(
        fields=[

            TextField(
                "nome_fantasia",
                weight=2.0,
            ),

            TagField(
                "bandeira"
            ),

            TagField(
                "cidade"
            ),

            TagField(
                "estado"
            ),

            TagField(
                "bairro"
            ),

            NumericField(
                "media_avaliacao",
                sortable=True,
            ),

            NumericField(
                "total_avaliacoes",
                sortable=True,
            ),

            NumericField(
                "gasolina_comum",
                sortable=True,
            ),

            NumericField(
                "etanol",
                sortable=True,
            ),

            NumericField(
                "diesel_s10",
                sortable=True,
            ),

            GeoField(
                "location"
            ),
        ],
        definition=IndexDefinition(
            prefix=["posto:"],
            index_type=IndexType.HASH,
        ),
    )

    print(
        f"[REDIS] Estruturas carregadas "
        f"com {len(snapshot)} postos."
    )

    print(
        "[REDIS] Rankings criados:"
    )

    print(
        "- ranking:preco:*"
    )

    print(
        "- ranking:variacao:*"
    )

    print(
        "- ranking:combustivel:buscas"
    )

    print(
        "- ranking:buscas:bairro"
    )

    print(
        "- postos:geo"
    )

    print(
        "- ts:posto:*"
    )

    print(
        "- idx:postos"
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    main()
