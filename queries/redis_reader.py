import os
import time
from datetime import datetime
from typing import List, Tuple

from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

# =========================================================
# ENVIRONMENT
# =========================================================

load_dotenv(".env.local")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# =========================================================
# REDIS CONNECTION
# =========================================================

def get_redis() -> Redis:

    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

# =========================================================
# HELPERS
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


def posto_bairro(
    redis: Redis,
    posto_id: str,
) -> str:

    bairro = redis.hget(
        f"posto:{posto_id}",
        "bairro",
    )

    return bairro or "-"


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
# CONSULTAS PRINCIPAIS
# =========================================================

# ---------------------------------------------------------
# 1. POSTOS MAIS BARATOS POR REGIÃO
# ---------------------------------------------------------

def postos_mais_baratos(
    redis: Redis,
    combustivel: str,
    bairro: str,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrange(
        f"ranking:preco:{combustivel}:{bairro}",
        0,
        n - 1,
        withscores=True,
    )


# ---------------------------------------------------------
# 2. COMBUSTÍVEIS EM ALTA
# ---------------------------------------------------------

def combustiveis_em_alta(
    redis: Redis,
    n: int = 5,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:combustivel:buscas",
        0,
        n - 1,
        withscores=True,
    )


# ---------------------------------------------------------
# 3. BAIRROS MAIS BUSCADOS
# ---------------------------------------------------------

def bairros_mais_buscados(
    redis: Redis,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        "ranking:buscas:bairro",
        0,
        n - 1,
        withscores=True,
    )


# ---------------------------------------------------------
# 4. MAIOR VARIAÇÃO RECENTE
# ---------------------------------------------------------

def maior_variacao_preco(
    redis: Redis,
    combustivel: str,
    n: int = 10,
) -> List[Tuple[str, float]]:

    return redis.zrevrange(
        f"ranking:variacao:{combustivel}",
        0,
        n - 1,
        withscores=True,
    )


# =========================================================
# REDISEARCH
# =========================================================

def busca_postos_tempo_real(
    redis: Redis,
    bairro: str,
    preco_maximo: float,
    limit: int = 10,
):

    query_parts = []

    if bairro.strip():

        query_parts.append(
            f"@bairro:{{{bairro}}}"
        )

    query_text = (
        " ".join(query_parts)
        if query_parts
        else "*"
    )

    query = (
        Query(query_text)
        .add_filter(
            NumericFilter(
                "gasolina_comum",
                0,
                preco_maximo,
            )
        )
        .sort_by(
            "gasolina_comum",
            asc=True,
        )
        .paging(0, limit)
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
    combustivel: str,
):

    key = (
        f"ts:posto:{posto_id}:{combustivel}"
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

    redis = get_redis()

    print(
        "[READER] Radar Combustível iniciado."
    )

    while True:

        # =================================================
        # 1. POSTOS MAIS BARATOS
        # =================================================

        print_block(
            "POSTOS COM MENOR PREÇO POR REGIÃO"
        )

        bairros = [
            "Pinheiros",
            "Centro",
            "Moema",
            "Vila_Mariana",
        ]

        for bairro in bairros:

            print(
                f"\nBairro: {bairro}"
            )

            results = postos_mais_baratos(
                redis,
                "gasolina_comum",
                bairro,
                5,
            )

            if not results:

                print(
                    "Sem dados."
                )

                continue

            for idx, (posto_id, preco) in enumerate(
                results,
                start=1,
            ):

                print(
                    f"{idx:02d}. "
                    f"{posto_nome(redis, posto_id)} "
                    f"| {posto_cidade(redis, posto_id)} "
                    f"-> R$ {round(preco, 3)}"
                )

        # =================================================
        # 2. COMBUSTÍVEIS EM ALTA
        # =================================================

        print_block(
            "COMBUSTÍVEIS MAIS BUSCADOS"
        )

        combustiveis = combustiveis_em_alta(
            redis,
            5,
        )

        if not combustiveis:

            print(
                "Sem dados."
            )

        else:

            for idx, (comb, score) in enumerate(
                combustiveis,
                start=1,
            ):

                print(
                    f"{idx:02d}. "
                    f"{comb} "
                    f"-> {int(score)} buscas"
                )

        # =================================================
        # 3. BAIRROS MAIS BUSCADOS
        # =================================================

        print_block(
            "BAIRROS COM MAIOR VOLUME DE BUSCAS"
        )

        bairros_rank = bairros_mais_buscados(
            redis,
            10,
        )

        if not bairros_rank:

            print(
                "Sem dados."
            )

        else:

            for idx, (bairro, score) in enumerate(
                bairros_rank,
                start=1,
            ):

                print(
                    f"{idx:02d}. "
                    f"{bairro} "
                    f"-> {int(score)} buscas"
                )

        # =================================================
        # 4. MAIOR VARIAÇÃO DE PREÇO
        # =================================================

        print_block(
            "POSTOS COM MAIOR VARIAÇÃO RECENTE"
        )

        variacoes = maior_variacao_preco(
            redis,
            "gasolina_comum",
            10,
        )

        if not variacoes:

            print(
                "Sem dados."
            )

        else:

            for idx, (posto_id, delta) in enumerate(
                variacoes,
                start=1,
            ):

                print(
                    f"{idx:02d}. "
                    f"{posto_nome(redis, posto_id)} "
                    f"-> variação {round(delta, 3)}"
                )

        # =================================================
        # 5. CONSULTAS EM TEMPO REAL
        # =================================================

        print_block(
            "CONSULTAS EM TEMPO REAL"
        )

        try:

            result = busca_postos_tempo_real(
                redis,
                bairro="Pinheiros",
                preco_maximo=6.00,
                limit=5,
            )

            if result.total == 0:

                print(
                    "Nenhum resultado encontrado."
                )

            else:

                for doc in result.docs:

                    print(
                        f"{doc.id}"
                        f" | {getattr(doc, 'nome_fantasia', '-')}"
                        f" | bairro={getattr(doc, 'bairro', '-')}"
                        f" | gasolina={getattr(doc, 'gasolina_comum', '-')}"
                    )

        except Exception as exc:

            print(
                f"Falha RediSearch: {exc}"
            )

        # =================================================
        # 6. SÉRIE TEMPORAL
        # =================================================

        print_block(
            "SÉRIE TEMPORAL DE PREÇOS"
        )

        try:

            ranking = redis.zrange(
                "ranking:preco:gasolina_comum:Pinheiros",
                0,
                0,
            )

            if not ranking:

                print(
                    "Nenhum posto disponível."
                )

            else:

                posto_id = ranking[0]

                series = preco_series(
                    redis,
                    posto_id,
                    "gasolina_comum",
                )

                if not series:

                    print(
                        "Sem dados temporais."
                    )

                else:

                    for point in series[-10:]:

                        ts, value = point

                        dt = datetime.fromtimestamp(
                            int(ts) / 1000
                        )

                        print(
                            f"{dt} -> {value}"
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
