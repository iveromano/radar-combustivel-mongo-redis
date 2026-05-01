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

# Load .env.local first (for host development)
load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/?directConnection=true",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_NAME = "radar_combustivel"

# Mongo collections
COL_EVENTOS_PRECO = "eventos_preco"
COL_AVALIACOES = "avaliacoes_interacoes"
COL_BUSCAS = "buscas_usuarios"
COL_POSTOS = "postos"

# =========================================================
# HELPERS
# =========================================================

def posto_hash_key(posto_id: str) -> str:
    return f"posto:{posto_id}"


def ts_key(posto_id: str, metric: str) -> str:
    return f"ts:posto:{posto_id}:{metric}"


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

    except ResponseError as exc:

        msg = str(exc)

        if (
            "key does not exist" not in msg
            and "TSDB: the key does not exist" not in msg
        ):
            raise

        redis.execute_command(
            "TS.CREATE",
            key,
            "RETENTION",
            604800000,
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            *sum(([k, v] for k, v in labels.items()), []),
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
# EVENT NORMALIZATION
# =========================================================

def normalize_price_event(raw: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "event_type": "price_update",
        "posto_id": str(raw["posto_id"]),
        "combustivel": raw.get("combustivel"),
        "preco_novo": float(raw.get("preco_novo", 0)),
        "variacao_pct": float(raw.get("variacao_pct", 0)),
        "fonte": raw.get("fonte", ""),
        "ts": int(raw["ocorrido_em"].timestamp() * 1000),
    }


def normalize_rating_event(raw: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "event_type": "rating",
        "posto_id": str(raw["posto_id"]),
        "nota": raw.get("nota"),
        "tipo": raw.get("tipo"),
        "util_count": int(raw.get("util_count", 0)),
        "ts": int(raw["created_at"].timestamp() * 1000),
    }


def normalize_search_event(raw: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "event_type": "search",
        "cidade": raw.get("cidade"),
        "estado": raw.get("estado"),
        "tipo_combustivel": raw.get("tipo_combustivel"),
        "resultado_count": int(raw.get("resultado_count", 0)),
        "latencia_ms": int(raw.get("latencia_ms", 0)),
        "ts": int(raw["consultado_em"].timestamp() * 1000),
    }


# =========================================================
# REDIS APPLY
# =========================================================

def apply_price_event(redis: Redis, event: Dict[str, Any]) -> None:

    r_hash = posto_hash_key(event["posto_id"])

    fuel_field = event["combustivel"].lower()

    redis.hset(
        r_hash,
        fuel_field,
        event["preco_novo"],
    )

    # ranking combustível mais barato
    redis.zadd(
        f"ranking:combustivel:{fuel_field}:menor_preco",
        {event["posto_id"]: event["preco_novo"]},
    )

    # ranking maior queda
    redis.zadd(
        f"ranking:combustivel:{fuel_field}:maior_queda",
        {event["posto_id"]: abs(event["variacao_pct"])},
    )

    ensure_ts_add(
        redis,
        ts_key(event["posto_id"], "price_updates"),
        event["ts"],
        event["preco_novo"],
        {
            "posto_id": event["posto_id"],
            "metric": "price_updates",
        },
    )

    print(
        f"[REDIS] PRICE UPDATE | "
        f"{event['posto_id']} | "
        f"{fuel_field} = {event['preco_novo']}"
    )


def apply_rating_event(redis: Redis, event: Dict[str, Any]) -> None:

    r_hash = posto_hash_key(event["posto_id"])

    # compartilhamento
    if event["tipo"] == "compartilhamento":

        shares = redis.hincrby(r_hash, "shares", 1)

        redis.zadd(
            "ranking:postos:shares",
            {event["posto_id"]: shares},
        )

        ensure_ts_add(
            redis,
            ts_key(event["posto_id"], "shares"),
            event["ts"],
            shares,
            {
                "posto_id": event["posto_id"],
                "metric": "shares",
            },
        )

        print(
            f"[REDIS] SHARE | "
            f"{event['posto_id']} | "
            f"shares={shares}"
        )

        return

    # avaliação
    nota = event.get("nota")

    if nota is not None:

        redis.hincrbyfloat(
            r_hash,
            "rating_sum",
            float(nota),
        )

        redis.hincrby(
            r_hash,
            "rating_count",
            1,
        )

        rating_sum = float(
            redis.hget(r_hash, "rating_sum") or 0
        )

        rating_count = int(
            redis.hget(r_hash, "rating_count") or 1
        )

        avg = round(
            rating_sum / max(rating_count, 1),
            2,
        )

        redis.hset(
            r_hash,
            "media_avaliacao",
            avg,
        )

        redis.zadd(
            "ranking:postos:avaliacao",
            {event["posto_id"]: avg},
        )

        print(
            f"[REDIS] RATING | "
            f"{event['posto_id']} | "
            f"avg={avg}"
        )


def apply_search_event(redis: Redis, event: Dict[str, Any]) -> None:

    fuel = event["tipo_combustivel"]

    # ranking combustível mais buscado
    score = redis.zincrby(
        "ranking:combustivel:buscas",
        1,
        fuel,
    )

    # analytics por cidade
    city_key = (
        f"analytics:buscas:{event['estado']}:{event['cidade']}"
    )

    redis.hincrby(city_key, fuel, 1)

    print(
        f"[REDIS] SEARCH | "
        f"{fuel} | "
        f"score={int(float(score))}"
    )


# =========================================================
# EVENT HANDLERS
# =========================================================

def handle_price_event(
    redis: Redis,
    raw_event: Dict[str, Any],
) -> None:

    event = normalize_price_event(raw_event)

    print(
        f"[EVENT] PRICE_UPDATE | "
        f"{event['posto_id']} | "
        f"{event['combustivel']} | "
        f"{event['preco_novo']}"
    )

    apply_price_event(redis, event)


def handle_rating_event(
    redis: Redis,
    raw_event: Dict[str, Any],
) -> None:

    event = normalize_rating_event(raw_event)

    print(
        f"[EVENT] RATING | "
        f"{event['posto_id']} | "
        f"{event['tipo']}"
    )

    apply_rating_event(redis, event)


def handle_search_event(
    redis: Redis,
    raw_event: Dict[str, Any],
) -> None:

    event = normalize_search_event(raw_event)

    print(
        f"[EVENT] SEARCH | "
        f"{event['tipo_combustivel']} | "
        f"{event['cidade']}"
    )

    apply_search_event(redis, event)


# =========================================================
# BACKFILL
# =========================================================

def backfill_existing(
    mongo,
    redis: Redis,
    limit: int = 50000,
) -> None:

    processed = 0

    # -----------------------------------------------------
    # PREÇOS
    # -----------------------------------------------------

    col_precos = mongo[DB_NAME][COL_EVENTOS_PRECO]

    for doc in (
        col_precos.find({})
        .sort("ocorrido_em", 1)
        .limit(limit)
    ):

        handle_price_event(redis, doc)

        processed += 1

    # -----------------------------------------------------
    # AVALIAÇÕES
    # -----------------------------------------------------

    col_avaliacoes = mongo[DB_NAME][COL_AVALIACOES]

    for doc in (
        col_avaliacoes.find({})
        .sort("created_at", 1)
        .limit(limit)
    ):

        handle_rating_event(redis, doc)

        processed += 1

    # -----------------------------------------------------
    # BUSCAS
    # -----------------------------------------------------

    col_buscas = mongo[DB_NAME][COL_BUSCAS]

    for doc in (
        col_buscas.find({})
        .sort("consultado_em", 1)
        .limit(limit)
    ):

        handle_search_event(redis, doc)

        processed += 1

    print(
        f"[CONSUMER] Backfill concluído: "
        f"{processed} eventos."
    )


# =========================================================
# MAIN
# =========================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Consome eventos do MongoDB "
            "e publica no Redis."
        )
    )

    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Não processa eventos já existentes.",
    )

    args = parser.parse_args()

    mongo = MongoClient(MONGO_URI)

    redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    if not args.skip_backfill:
        backfill_existing(mongo, redis)

    print("[CONSUMER] MongoDB Change Streams conectados")
    print("[CONSUMER] Aguardando eventos...")

    while True:

        try:

            col_precos = mongo[DB_NAME][COL_EVENTOS_PRECO]
            col_avaliacoes = mongo[DB_NAME][COL_AVALIACOES]
            col_buscas = mongo[DB_NAME][COL_BUSCAS]

            with col_precos.watch(
                [{"$match": {"operationType": "insert"}}],
                full_document="updateLookup",
            ) as stream_precos, \
                 col_avaliacoes.watch(
                     [{"$match": {"operationType": "insert"}}],
                     full_document="updateLookup",
                 ) as stream_avaliacoes, \
                 col_buscas.watch(
                     [{"$match": {"operationType": "insert"}}],
                     full_document="updateLookup",
                 ) as stream_buscas:

                while True:

                    for change in stream_precos.try_next(),:

                        if change:
                            handle_price_event(
                                redis,
                                change["fullDocument"],
                            )

                    for change in stream_avaliacoes.try_next(),:

                        if change:
                            handle_rating_event(
                                redis,
                                change["fullDocument"],
                            )

                    for change in stream_buscas.try_next(),:

                        if change:
                            handle_search_event(
                                redis,
                                change["fullDocument"],
                            )

                    time.sleep(0.2)

        except Exception as exc:

            print(
                f"[CONSUMER] Reconectando após erro: {exc}"
            )

            time.sleep(2)


if __name__ == "__main__":
    main()