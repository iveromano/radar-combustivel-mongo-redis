from typing import List, Tuple

from redis import Redis
from redis.commands.search.query import NumericFilter, Query

from config import REDIS_HOST, REDIS_PORT


def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def posto_nome(redis: Redis, posto_id: str) -> str:
    return redis.hget(f'posto:{posto_id}', 'nome_fantasia') or posto_id


def posto_bairro(redis: Redis, posto_id: str) -> str:
    return redis.hget(f'posto:{posto_id}', 'bairro') or '-'


def posto_cidade(redis: Redis, posto_id: str) -> str:
    return redis.hget(f'posto:{posto_id}', 'cidade') or '-'


def postos_mais_baratos_uf(redis: Redis, combustivel: str, uf: str, n: int = 10) -> List[Tuple[str, float]]:
    return [(m, s) for m, s in redis.zrange(f'ranking:preco:{combustivel}:{uf}', 0, n - 1, withscores=True) if m != '__seed__']


def postos_mais_baratos_cidade(redis: Redis, combustivel: str, cidade: str, n: int = 10) -> List[Tuple[str, float]]:
    return [(m, s) for m, s in redis.zrange(f'ranking:preco:{combustivel}:{cidade}', 0, n - 1, withscores=True) if m != '__seed__']


def combustiveis_em_alta(redis: Redis, n: int = 5) -> List[Tuple[str, float]]:
    return [(m, s) for m, s in redis.zrevrange('ranking:combustivel:buscas', 0, n - 1, withscores=True) if m != '__seed__']


def cidades_mais_buscadas(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return [(m, s) for m, s in redis.zrevrange('ranking:buscas:cidade', 0, n - 1, withscores=True) if m != '__seed__']


def maior_variacao_preco(redis: Redis, combustivel: str, n: int = 10) -> List[Tuple[str, float]]:
    return [(m, s) for m, s in redis.zrevrange(f'ranking:variacao:{combustivel}', 0, n - 1, withscores=True) if m != '__seed__']


def postos_mais_engajados(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return [(m, s) for m, s in redis.zrevrange('ranking:interacoes:postos', 0, n - 1, withscores=True) if m != '__seed__']


def busca_postos_tempo_real(redis: Redis, estado: str, preco_maximo: float, limit: int = 10):
    query_text = f'@estado:{{{estado}}}' if estado.strip() else '*'
    query = Query(query_text).add_filter(NumericFilter('gasolina_comum', 0, preco_maximo)).sort_by('gasolina_comum', asc=True).paging(0, limit)
    return redis.ft('idx:postos').search(query)


def postos_proximos(redis: Redis, lon: float, lat: float, raio_km: int = 5, n: int = 10):
    return redis.execute_command('GEOSEARCH', 'geo:postos', 'FROMLONLAT', lon, lat, 'BYRADIUS', raio_km, 'km', 'ASC', 'COUNT', n)


def preco_series(redis: Redis, posto_id: str, combustivel: str):
    return redis.execute_command('TS.RANGE', f'ts:posto:{posto_id}:{combustivel}', '-', '+', 'AGGREGATION', 'avg', '60000')
