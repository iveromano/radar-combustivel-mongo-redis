import os
import time
from typing import List, Tuple

from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

# =========================================================
# ENVIRONMENT
# =========================================================

# Load .env.local first
load_dotenv(".env.local")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# =========================================================
# RANKINGS
# =========================================================

def top_postos_avaliados(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:postos:avaliacao",
        0,
        n - 1,
        withscores=True,
    )


def top_postos_compartilhados(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:postos:shares",
        0,
        n - 1,
        withscores=True,
    )


def combustiveis_mais_buscados(
    redis: Redis,
    n: int = 5,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:combustivel:buscas",
        0,
        n - 1,
        withscores=True,
    )


def postos_mais_baratos(
    redis: Redis,
    combustivel: str,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrange(
        f"ranking:combustivel:{combustivel}:menor_preco",
        0,
        n - 1,
        withscores=True,
    )


# =========================================================
# HASH HELPERS
# =========================================================

def posto_nome(
    redis: Redis,
    posto_id: str,
) -> str:

    nome = redis.hget(
        f"posto:{posto_id}",
        "nome_fantasia",
    )

    return nome or posto_id


def posto_bandeira(
    redis: Redis,
    posto_id: str,
) -> str:

    bandeira = redis.hget(
        f"posto:{posto_id}",
        "bandeira",
    )

    return bandeira or "-"


def posto_cidade(
    redis: Redis,
    posto_id: str,
) -> str:

    cidade = redis.hget(
        f"posto:{posto_id}",
        "cidade",
    )

    return cidade or "-"


# =========================================================
# REDISEARCH
# =========================================================

def postos_ipiranga_sp(
    redis: Redis,
):

    query = (
        Query(
            "@bandeira:{Ipiranga} "
            "@estado:{SP}"
        )
        .add_filter(
            NumericFilter(
                "media_avaliacao",
                4,
                5,
            )
        )
        .sort_by(
            "gasolina_comum",
            asc=True,
        )
        .paging(0, 10)
    )

    return redis.ft(
        "idx:postos"
    ).search(query)


def postos_diesel_barato(
    redis: Redis,
):

    query = (
        Query(
            "@estado:{SP}"
        )
        .add_filter(
            NumericFilter(
                "diesel_s10",
                0,
                6.00,
            )
        )
        .sort_by(
            "diesel_s10",
            asc=True,
        )
        .paging(0, 10)
    )

    return redis.ft(
        "idx:postos"
    ).search(query)


# =========================================================
# REDISTIMESERIES
# =========================================================

def preco_series(
    redis: Redis,
    posto_id: str,
):

    key = (
        f"ts:posto:{posto_id}:price_updates"
    )

    return redis.execute_command(
        "TS.RANGE",
        key,
        "-",
        "+",
        "AGGREGATION",
        "avg",
        "60000",
    )


def shares_series(
    redis: Redis,
    posto_id: str,
):

    key = (
        f"ts:posto:{posto_id}:shares"
    )

    return redis.execute_command(
        "TS.RANGE",
        key,
        "-",
        "+",
        "AGGREGATION",
        "sum",
        "60000",
    )


# =========================================================
# OUTPUT
# =========================================================

def print_block(title: str) -> None:

    print("\n" + "=" * 80)

    print(title)

    print("=" * 80)


# =========================================================
# MAIN
# =========================================================

def main() -> None:

    redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    print(
        "[READER] Consultas Redis "
        "em tempo real iniciadas."
    )

    while True:

        # =================================================
        # TOP AVALIADOS
        # =================================================

        print_block(
            "Top 10 postos mais bem avaliados"
        )

        for idx, (member, score) in enumerate(
            top_postos_avaliados(redis),
            start=1,
        ):

            print(
                f"{idx:02d}. "
                f"{posto_nome(redis, member)} "
                f"({posto_bandeira(redis, member)}) "
                f"-> nota {round(score, 2)}"
            )

        # =================================================
        # TOP SHARES
        # =================================================

        print_block(
            "Top 10 postos mais compartilhados"
        )

        for idx, (member, score) in enumerate(
            top_postos_compartilhados(redis),
            start=1,
        ):

            print(
                f"{idx:02d}. "
                f"{posto_nome(redis, member)} "
                f"-> {int(score)} compartilhamentos"
            )

        # =================================================
        # COMBUSTÍVEIS MAIS BUSCADOS
        # =================================================

        print_block(
            "Top combustíveis mais buscados"
        )

        for idx, (member, score) in enumerate(
            combustiveis_mais_buscados(redis),
            start=1,
        ):

            print(
                f"{idx:02d}. "
                f"{member} "
                f"-> {int(score)} buscas"
            )

        # =================================================
        # GASOLINA MAIS BARATA
        # =================================================

        print_block(
            "Top 10 gasolina comum mais barata"
        )

        for idx, (member, score) in enumerate(
            postos_mais_baratos(
                redis,
                "gasolina_comum",
            ),
            start=1,
        ):

            print(
                f"{idx:02d}. "
                f"{posto_nome(redis, member)} "
                f"| {posto_cidade(redis, member)} "
                f"-> R$ {round(score, 3)}"
            )

        # =================================================
        # REDISEARCH
        # =================================================

        print_block(
            "Postos Ipiranga em SP "
            "com nota 4+"
        )

        try:

            result = postos_ipiranga_sp(redis)

            if result.total == 0:

                print(
                    "Nenhum resultado encontrado."
                )

            else:

                for doc in result.docs[:10]:

                    print(
                        f"{doc.id}"
                        f" | {getattr(doc, 'nome_fantasia', '-')}"
                        f" | gasolina={getattr(doc, 'gasolina_comum', '-')}"
                        f" | nota={getattr(doc, 'media_avaliacao', '-')}"
                    )

        except Exception as exc:

            print(
                f"Falha RediSearch: {exc}"
            )

        # =================================================
        # DIESEL BARATO
        # =================================================

        print_block(
            "Diesel S10 abaixo de R$ 6.00"
        )

        try:

            result = postos_diesel_barato(redis)

            if result.total == 0:

                print(
                    "Nenhum posto encontrado."
                )

            else:

                for doc in result.docs[:10]:

                    print(
                        f"{doc.id}"
                        f" | {getattr(doc, 'nome_fantasia', '-')}"
                        f" | diesel={getattr(doc, 'diesel_s10', '-')}"
                    )

        except Exception as exc:

            print(
                f"Falha RediSearch: {exc}"
            )

        # =================================================
        # TIMESERIES
        # =================================================

        print_block(
            "TimeSeries de preço "
            "(agregação por minuto)"
        )

        try:

            posto_exemplo = (
                redis.zrange(
                    "ranking:postos:avaliacao",
                    0,
                    0,
                )
            )

            if not posto_exemplo:

                print(
                    "Nenhum posto disponível."
                )

            else:

                posto_id = posto_exemplo[0]

                series = preco_series(
                    redis,
                    posto_id,
                )

                if not series:

                    print(
                        "Sem dados de série temporal."
                    )

                else:

                    for point in series[-10:]:

                        ts, value = point

                        print(
                            f"{ts} -> {value}"
                        )

        except Exception as exc:

            print(
                f"Falha TimeSeries: {exc}"
            )

        # =================================================
        # LOOP
        # =================================================

        time.sleep(5)


if __name__ == "__main__":
    main()