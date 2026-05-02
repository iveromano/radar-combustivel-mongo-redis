# mongodb_consumer.py

import argparse
import os
import time
from typing import Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError

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

COL_EVENTOS_PRECO = "eventos_preco"
COL_AVALIACOES = "avaliacoes_interacoes"
COL_BUSCAS = "buscas_usuarios"

# =========================================================
# REDIS HELPERS
# =========================================================

def posto_key(posto_id: str) -> str:

    return f"posto:{posto_id}"


def timeseries_key(
    posto_id: str,
    combustivel: str,
) -> str:

    return (
        f"ts:posto:{posto_id}:"
        f"{combustivel}"
    )


def ensure_ts_add(
    redis: Redis,
    key: str,
    ts: int,
    value: float,
    labels: Dict[str, str],
) -> None:

    try:

        redis.execute_command(
            "TS.ADD",
            key,
            ts,
            value,
            "ON_DUPLICATE",
            "LAST",
        )

    except ResponseError:

        redis.execute_command(
            "TS.CREATE",
            key,
            "RETENTION",
            604800000,
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            *sum(
                (
                    [k, v]
                    for k, v
                    in labels.items()
                ),
                [],
            ),
        )

        redis.execute_command(
            "TS.ADD",
            key,
            ts,
            value,
            "ON_DUPLICATE",
            "LAST",
        )

# =========================================================
# NORMALIZATION
# =========================================================

def normalize_price_event(
    raw: Dict[str, Any],
) -> Dict[str, Any]:

    combustivel = str(
        raw.get(
            "combustivel",
            "",
        )
    ).lower()

    return {

        "type": "price_update",

        "posto_id": str(
            raw["posto_id"]
        ),

        "combustivel": combustivel,

        "preco_novo": float(
            raw.get(
                "preco_novo",
                0,
            )
        ),

        "variacao_pct": float(
            raw.get(
                "variacao_pct",
                0,
            )
        ),

        "ts": int(
            raw["ocorrido_em"]
            .timestamp() * 1000
        ),
    }


def normalize_rating_event(
    raw: Dict[str, Any],
) -> Dict[str, Any]:

    return {

        "type": str(
            raw.get(
                "tipo",
                "",
            )
        ).lower(),

        "posto_id": str(
            raw["posto_id"]
        ),

        "nota": raw.get(
            "nota"
        ),

        "ts": int(
            raw["created_at"]
            .timestamp() * 1000
        ),
    }


def normalize_search_event(
    raw: Dict[str, Any],
) -> Dict[str, Any]:

    return {

        "type": "search",

        "bairro": raw.get(
            "bairro",
            "desconhecido",
        ),

        "cidade": raw.get(
            "cidade",
            "",
        ),

        "estado": raw.get(
            "estado",
            "",
        ),

        "combustivel": str(
            raw.get(
                "tipo_combustivel",
                "",
            )
        ).lower(),

        "latencia_ms": int(
            raw.get(
                "latencia_ms",
                0,
            )
        ),

        "resultado_count": int(
            raw.get(
                "resultado_count",
                0,
            )
        ),

        "ts": int(
            raw["consultado_em"]
            .timestamp() * 1000
        ),
    }

# =========================================================
# APPLY EVENTS
# =========================================================

def apply_price_event(
    redis: Redis,
    event: Dict[str, Any],
) -> None:

    posto_id = event["posto_id"]

    combustivel = (
        event["combustivel"]
    )

    preco = (
        event["preco_novo"]
    )

    variacao = abs(
        event["variacao_pct"]
    )

    ts = event["ts"]

    hash_key = posto_key(
        posto_id
    )

    bairro = redis.hget(
        hash_key,
        "bairro",
    )

    if not bairro:
        bairro = "desconhecido"

    # =====================================================
    # HASH
    # =====================================================

    redis.hset(
        hash_key,
        combustivel,
        preco,
    )

    # =====================================================
    # RANKING MENOR PREÇO POR BAIRRO
    # =====================================================

    redis.zadd(
        f"ranking:preco:{combustivel}:{bairro}",
        {
            posto_id: preco
        },
    )

    # =====================================================
    # MAIOR VARIAÇÃO
    # =====================================================

    redis.zadd(
        f"ranking:variacao:{combustivel}",
        {
            posto_id: variacao
        },
    )

    # =====================================================
    # TIMESERIES
    # =====================================================

    ensure_ts_add(
        redis,
        timeseries_key(
            posto_id,
            combustivel,
        ),
        ts,
        preco,
        {
            "posto_id": posto_id,
            "combustivel": combustivel,
        },
    )

    print(
        "[PRICE]"
        f" posto={posto_id}"
        f" combustivel={combustivel}"
        f" preco={preco}"
    )


def apply_rating_event(
    redis: Redis,
    event: Dict[str, Any],
) -> None:

    posto_id = event[
        "posto_id"
    ]

    event_type = event[
        "type"
    ]

    hash_key = posto_key(
        posto_id
    )

    # =====================================================
    # SHARE
    # =====================================================

    if event_type == "compartilhamento":

        shares = redis.hincrby(
            hash_key,
            "shares",
            1,
        )

        redis.zadd(
            "ranking:postos:shares",
            {
                posto_id: shares
            },
        )

        print(
            "[SHARE]"
            f" posto={posto_id}"
            f" total={shares}"
        )

        return

    # =====================================================
    # AVALIAÇÃO
    # =====================================================

    nota = event.get(
        "nota"
    )

    if nota is None:
        return

    redis.hincrbyfloat(
        hash_key,
        "rating_sum",
        float(nota),
    )

    redis.hincrby(
        hash_key,
        "rating_count",
        1,
    )

    rating_sum = float(
        redis.hget(
            hash_key,
            "rating_sum",
        ) or 0
    )

    rating_count = int(
        redis.hget(
            hash_key,
            "rating_count",
        ) or 1
    )

    media = round(
        rating_sum /
        max(rating_count, 1),
        2,
    )

    redis.hset(
        hash_key,
        "media_avaliacao",
        media,
    )

    redis.zadd(
        "ranking:postos:avaliacao",
        {
            posto_id: media
        },
    )

    print(
        "[RATING]"
        f" posto={posto_id}"
        f" media={media}"
    )


def apply_search_event(
    redis: Redis,
    event: Dict[str, Any],
) -> None:

    combustivel = (
        event["combustivel"]
    )

    bairro = (
        event["bairro"]
    )

    cidade = (
        event["cidade"]
    )

    estado = (
        event["estado"]
    )

    # =====================================================
    # COMBUSTÍVEL MAIS BUSCADO
    # =====================================================

    redis.zincrby(
        "ranking:combustivel:buscas",
        1,
        combustivel,
    )

    # =====================================================
    # BAIRRO MAIS BUSCADO
    # =====================================================

    redis.zincrby(
        "ranking:buscas:bairro",
        1,
        bairro,
    )

    # =====================================================
    # ANALYTICS LOCAL
    # =====================================================

    redis.hincrby(
        (
            f"analytics:buscas:"
            f"{estado}:{cidade}"
        ),
        combustivel,
        1,
    )

    print(
        "[SEARCH]"
        f" combustivel={combustivel}"
        f" bairro={bairro}"
    )

# =========================================================
# HANDLERS
# =========================================================

def handle_price_event(
    redis: Redis,
    raw: Dict[str, Any],
) -> None:

    apply_price_event(
        redis,
        normalize_price_event(
            raw
        ),
    )


def handle_rating_event(
    redis: Redis,
    raw: Dict[str, Any],
) -> None:

    apply_rating_event(
        redis,
        normalize_rating_event(
            raw
        ),
    )


def handle_search_event(
    redis: Redis,
    raw: Dict[str, Any],
) -> None:

    apply_search_event(
        redis,
        normalize_search_event(
            raw
        ),
    )

# =========================================================
# BACKFILL
# =========================================================

def backfill_existing(
    mongo,
    redis: Redis,
    limit: int = 50000,
) -> None:

    processed = 0

    # =====================================================
    # PREÇOS
    # =====================================================

    for doc in (
        mongo[DB_NAME][COL_EVENTOS_PRECO]
        .find({})
        .sort("ocorrido_em", 1)
        .limit(limit)
    ):

        handle_price_event(
            redis,
            doc,
        )

        processed += 1

    # =====================================================
    # AVALIAÇÕES
    # =====================================================

    for doc in (
        mongo[DB_NAME][COL_AVALIACOES]
        .find({})
        .sort("created_at", 1)
        .limit(limit)
    ):

        handle_rating_event(
            redis,
            doc,
        )

        processed += 1

    # =====================================================
    # BUSCAS
    # =====================================================

    for doc in (
        mongo[DB_NAME][COL_BUSCAS]
        .find({})
        .sort("consultado_em", 1)
        .limit(limit)
    ):

        handle_search_event(
            redis,
            doc,
        )

        processed += 1

    print(
        "[BACKFILL]"
        f" eventos={processed}"
    )

# =========================================================
# MAIN
# =========================================================

def main() -> None:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--skip-backfill",
        action="store_true",
    )

    args = parser.parse_args()

    mongo = MongoClient(
        MONGO_URI
    )

    redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    # =====================================================
    # BACKFILL
    # =====================================================

    if not args.skip_backfill:

        backfill_existing(
            mongo,
            redis,
        )

    print(
        "[CONSUMER] Change Streams ativos"
    )

    # =====================================================
    # STREAM LOOP
    # =====================================================

    while True:

        try:

            col_precos = (
                mongo[DB_NAME]
                [COL_EVENTOS_PRECO]
            )

            col_avaliacoes = (
                mongo[DB_NAME]
                [COL_AVALIACOES]
            )

            col_buscas = (
                mongo[DB_NAME]
                [COL_BUSCAS]
            )

            with col_precos.watch(
                [
                    {
                        "$match": {
                            "operationType": "insert"
                        }
                    }
                ],
                full_document="updateLookup",
            ) as stream_precos, \
                 col_avaliacoes.watch(
                     [
                         {
                             "$match": {
                                 "operationType": "insert"
                             }
                         }
                     ],
                     full_document="updateLookup",
                 ) as stream_avaliacoes, \
                 col_buscas.watch(
                     [
                         {
                             "$match": {
                                 "operationType": "insert"
                             }
                         }
                     ],
                     full_document="updateLookup",
                 ) as stream_buscas:

                while True:

                    change = (
                        stream_precos.try_next()
                    )

                    if change:

                        handle_price_event(
                            redis,
                            change[
                                "fullDocument"
                            ],
                        )

                    change = (
                        stream_avaliacoes
                        .try_next()
                    )

                    if change:

                        handle_rating_event(
                            redis,
                            change[
                                "fullDocument"
                            ],
                        )

                    change = (
                        stream_buscas
                        .try_next()
                    )

                    if change:

                        handle_search_event(
                            redis,
                            change[
                                "fullDocument"
                            ],
                        )

                    time.sleep(0.2)

        except Exception as exc:

            print(
                "[CONSUMER ERROR]",
                exc,
            )

            time.sleep(2)


if __name__ == "__main__":
    main()
