import os
import time
from typing import List, Tuple

from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

from config import REDIS_HOST, REDIS_PORT

# Load .env.local first (for host development), fallback to .env (for Docker)
load_dotenv('.env.local', override=True)
load_dotenv(override=True)


def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def posto_nome(redis: Redis, posto_id: str) -> str:
    name = redis.hget(f"posto:{posto_id}", "nome_fantasia")
    return name or posto_id


def posto_bairro(redis: Redis, posto_id: str) -> str:
    bairro = redis.hget(f"posto:{posto_id}", "bairro")
    return bairro or "-"


def posto_cidade(redis: Redis, posto_id: str) -> str:
    cidade = redis.hget(f"posto:{posto_id}", "cidade")
    return cidade or "-"


def posto_estado(redis: Redis, posto_id: str) -> str:
    estado = redis.hget(f"posto:{posto_id}", "estado")
    return estado or "-"


def postos_mais_baratos_uf(redis: Redis, combustivel: str, uf: str, n: int = 10) -> List[Tuple[str, float]]:
    return [
        (m, s)
        for m, s in redis.zrange(f"ranking:preco:{combustivel}:{uf}", 0, n - 1, withscores=True)
        if m != "__seed__"
    ]


def postos_mais_baratos_cidade(redis: Redis, combustivel: str, cidade: str, n: int = 10) -> List[Tuple[str, float]]:
    return [
        (m, s)
        for m, s in redis.zrange(f"ranking:preco:{combustivel}:{cidade}", 0, n - 1, withscores=True)
        if m != "__seed__"
    ]


def combustiveis_em_alta(redis: Redis, n: int = 5) -> List[Tuple[str, float]]:
    return [
        (m, s)
        for m, s in redis.zrevrange("ranking:combustivel:buscas", 0, n - 1, withscores=True)
        if m != "__seed__"
    ]


def cidades_mais_buscadas(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return [
        (m, s)
        for m, s in redis.zrevrange("ranking:buscas:cidade", 0, n - 1, withscores=True)
        if m != "__seed__"
    ]


def maior_variacao_preco(redis: Redis, combustivel: str, n: int = 10) -> List[Tuple[str, float]]:
    return [
        (m, s)
        for m, s in redis.zrevrange(f"ranking:variacao:{combustivel}", 0, n - 1, withscores=True)
        if m != "__seed__"
    ]


def postos_mais_engajados(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return [
        (m, s)
        for m, s in redis.zrevrange("ranking:interacoes:postos", 0, n - 1, withscores=True)
        if m != "__seed__"
    ]


def busca_postos_tempo_real(redis: Redis, estado: str, preco_maximo: float, limit: int = 10):
    query_text = f"@estado:{{{estado}}}" if estado.strip() else "*"
    query = (
        Query(query_text)
        .add_filter(NumericFilter("gasolina_comum", 0, preco_maximo))
        .sort_by("gasolina_comum", asc=True)
        .paging(0, limit)
    )
    return redis.ft("idx:postos").search(query)


def postos_proximos(redis: Redis, lon: float, lat: float, raio_km: int = 5, n: int = 10):
    return redis.execute_command(
        "GEOSEARCH",
        "geo:postos",
        "FROMLONLAT",
        lon,
        lat,
        "BYRADIUS",
        raio_km,
        "km",
        "ASC",
        "COUNT",
        n,
    )


def preco_series(redis: Redis, posto_id: str, combustivel: str):
    return redis.execute_command(
        "TS.RANGE",
        f"ts:posto:{posto_id}:{combustivel}",
        "-",
        "+",
        "AGGREGATION",
        "avg",
        "60000",
    )


def print_block(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    redis = get_redis()
    print("[READER] Consultas em tempo real iniciadas para Radar Combustível.")

    while True:
        print_block("Top 10 postos mais baratos por UF (gasolina_comum / SP)")
        try:
            rows = postos_mais_baratos_uf(redis, "gasolina_comum", "sp", 10)
            if not rows:
                print("Sem dados.")
            else:
                for idx, (member, score) in enumerate(rows, start=1):
                    print(f"{idx:02d}. {posto_nome(redis, member)} ({member}) -> R$ {score:.3f}")
        except Exception as exc:
            print(f"Falha na consulta por UF: {exc}")

        print_block("Top 10 postos mais baratos por cidade (gasolina_comum / alves)")
        try:
            rows = postos_mais_baratos_cidade(redis, "gasolina_comum", "alves", 10)
            if not rows:
                print("Sem dados.")
            else:
                for idx, (member, score) in enumerate(rows, start=1):
                    print(f"{idx:02d}. {posto_nome(redis, member)} ({member}) -> R$ {score:.3f}")
        except Exception as exc:
            print(f"Falha na consulta por cidade: {exc}")

        print_block("Top 5 combustíveis mais buscados")
        try:
            rows = combustiveis_em_alta(redis, 5)
            if not rows:
                print("Sem dados.")
            else:
                for idx, (member, score) in enumerate(rows, start=1):
                    print(f"{idx:02d}. {member} -> {int(score)} buscas")
        except Exception as exc:
            print(f"Falha no ranking de combustíveis: {exc}")

        print_block("Top 10 cidades mais buscadas")
        try:
            rows = cidades_mais_buscadas(redis, 10)
            if not rows:
                print("Sem dados.")
            else:
                for idx, (member, score) in enumerate(rows, start=1):
                    print(f"{idx:02d}. {member} -> {int(score)} buscas")
        except Exception as exc:
            print(f"Falha no ranking de cidades: {exc}")

        print_block("Top 10 postos com maior variação recente")
        try:
            rows = maior_variacao_preco(redis, "gasolina_comum", 10)
            if not rows:
                print("Sem dados.")
            else:
                for idx, (member, score) in enumerate(rows, start=1):
                    print(f"{idx:02d}. {posto_nome(redis, member)} ({member}) -> variação {score:.3f}")
        except Exception as exc:
            print(f"Falha na variação de preço: {exc}")

        print_block("Top 10 postos mais engajados")
        try:
            rows = postos_mais_engajados(redis, 10)
            if not rows:
                print("Sem dados.")
            else:
                for idx, (member, score) in enumerate(rows, start=1):
                    print(f"{idx:02d}. {posto_nome(redis, member)} ({member}) -> engajamento {int(score)}")
        except Exception as exc:
            print(f"Falha no ranking de engajamento: {exc}")

        print_block("Busca por proximidade (GEOSEARCH)")
        try:
            rows = postos_proximos(redis, lon=-46.633308, lat=-23.550520, raio_km=5, n=10)
            if not rows:
                print("Sem dados.")
            else:
                for idx, member in enumerate(rows, start=1):
                    print(
                        f"{idx:02d}. {posto_nome(redis, member)} ({member}) | "
                        f"{posto_bairro(redis, member)} | {posto_cidade(redis, member)} | {posto_estado(redis, member)}"
                    )
        except Exception as exc:
            print(f"Falha na busca por proximidade: {exc}")

        print_block("Série temporal do posto exemplo")
        try:
            series = preco_series(redis, "245", "gasolina_comum")
            if not series:
                print("Sem dados de série temporal para ts:posto:245:gasolina_comum.")
            else:
                for point in series[-10:]:
                    ts, value = point
                    print(f"{ts} -> {value}")
        except Exception as exc:
            print(f"Falha na TimeSeries: {exc}")

        time.sleep(5)


if __name__ == "__main__":
    main()
