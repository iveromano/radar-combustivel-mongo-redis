import os
from typing import Dict, Iterable

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

load_dotenv('.env.local')
load_dotenv()

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/?directConnection=true')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
DB_NAME = os.getenv('MONGO_DB', 'radar_combustivel')

POSTOS = 'postos'
LOCALIZACOES = 'localizacoes_postos'
COMBUSTIVEIS = ('gasolina_comum', 'etanol', 'diesel_s10', 'gnv')
RETENTION_MS = 604800000


def normalize(value: str) -> str:
    return str(value or '').strip().lower().replace(' ', '_')


def load_postos_snapshot() -> Dict[str, dict]:
    mongo = MongoClient(MONGO_URI)
    postos = mongo[DB_NAME][POSTOS]
    localizacoes = mongo[DB_NAME][LOCALIZACOES]

    local_por_posto = {str(doc['posto_id']): doc for doc in localizacoes.find({})}
    snapshot: Dict[str, dict] = {}

    for posto in postos.find({}):
        posto_id = str(posto['_id'])
        loc = local_por_posto.get(posto_id, {})
        endereco = posto.get('endereco', {}) or {}
        geo_doc = loc.get('geo') or posto.get('location') or {}
        coords = geo_doc.get('coordinates', [0, 0])
        lon = coords[0] if len(coords) > 0 else 0
        lat = coords[1] if len(coords) > 1 else 0

        snapshot[posto_id] = {
            'posto_id': posto_id,
            'cnpj': posto.get('cnpj', ''),
            'nome_fantasia': posto.get('nome_fantasia', ''),
            'bandeira': normalize(posto.get('bandeira', '')),
            'bairro': normalize(loc.get('bairro') or endereco.get('bairro', '')),
            'cidade': normalize(loc.get('municipio') or endereco.get('cidade', '')),
            'estado': normalize(loc.get('uf') or endereco.get('estado', '')),
            'codigo_ibge': str(loc.get('codigo_ibge', '')),
            'ativo': 1 if posto.get('ativo') else 0,
            'telefone': posto.get('telefone', ''),
            'location': f'{lon},{lat}',
            'longitude': float(lon),
            'latitude': float(lat),
            'gasolina_comum': 999.0,
            'etanol': 999.0,
            'diesel_s10': 999.0,
            'gnv': 999.0,
            'nota_sum': 0.0,
            'nota_count': 0,
            'engajamento': 0,
        }

    return snapshot


def ensure_geo(redis: Redis, snapshot: Dict[str, dict]) -> None:
    for posto_id, item in snapshot.items():
        lon = item['longitude']
        lat = item['latitude']
        if lon == 0 and lat == 0:
            continue
        redis.execute_command('GEOADD', 'geo:postos', lon, lat, posto_id)


def ensure_timeseries(redis: Redis, posto_id: str, combustivel: str) -> None:
    ts = f'ts:posto:{posto_id}:{combustivel}'
    try:
        redis.execute_command(
            'TS.CREATE',
            ts,
            'RETENTION', RETENTION_MS,
            'DUPLICATE_POLICY', 'LAST',
            'LABELS',
            'posto_id', posto_id,
            'combustivel', combustivel,
            'metric', 'preco',
        )
    except Exception:
        pass


def ensure_global_timeseries(redis: Redis) -> None:
    specs = [
        ('ts:buscas:latencia_ms', ('metric', 'latencia_ms', 'scope', 'buscas')),
        ('ts:buscas:resultado_count', ('metric', 'resultado_count', 'scope', 'buscas')),
    ]
    for key, labels in specs:
        try:
            redis.execute_command('TS.CREATE', key, 'RETENTION', RETENTION_MS, 'DUPLICATE_POLICY', 'LAST', 'LABELS', *labels)
        except Exception:
            pass


def ensure_ranking_placeholders(redis: Redis, snapshot: Dict[str, dict]) -> None:
    ufs = sorted({item['estado'] for item in snapshot.values() if item['estado']})
    cidades = sorted({item['cidade'] for item in snapshot.values() if item['cidade']})

    for uf in ufs:
        for combustivel in COMBUSTIVEIS:
            key = f'ranking:preco:{combustivel}:{uf}'
            redis.zadd(key, {'__seed__': 999999.0}, nx=True)

    for cidade in cidades:
        for combustivel in COMBUSTIVEIS:
            key = f'ranking:preco:{combustivel}:{cidade}'
            redis.zadd(key, {'__seed__': 999999.0}, nx=True)

    for combustivel in COMBUSTIVEIS:
        redis.zadd(f'ranking:variacao:{combustivel}', {'__seed__': 0.0}, nx=True)

    redis.zadd('ranking:combustivel:buscas', {'__seed__': 0.0}, nx=True)
    redis.zadd('ranking:buscas:cidade', {'__seed__': 0.0}, nx=True)
    redis.zadd('ranking:interacoes:postos', {'__seed__': 0.0}, nx=True)


def ensure_search_index(redis: Redis) -> None:
    try:
        redis.ft('idx:postos').dropindex(delete_documents=False)
    except Exception:
        pass

    redis.ft('idx:postos').create_index(
        fields=[
            TextField('nome_fantasia', weight=2.0),
            TagField('bandeira'),
            TagField('bairro'),
            TagField('cidade'),
            TagField('estado'),
            TagField('codigo_ibge'),
            NumericField('ativo', sortable=True),
            NumericField('gasolina_comum', sortable=True),
            NumericField('etanol', sortable=True),
            NumericField('diesel_s10', sortable=True),
            NumericField('gnv', sortable=True),
            NumericField('engajamento', sortable=True),
            GeoField('location'),
        ],
        definition=IndexDefinition(prefix=['posto:'], index_type=IndexType.HASH),
    )


def seed_hashes(redis: Redis, snapshot: Dict[str, dict]) -> None:
    for posto_id, item in snapshot.items():
        redis.hset(f'posto:{posto_id}', mapping=item)
        for combustivel in COMBUSTIVEIS:
            ensure_timeseries(redis, posto_id, combustivel)


def remove_seed_members(redis: Redis, keys: Iterable[str]) -> None:
    for key in keys:
        try:
            redis.zrem(key, '__seed__')
        except Exception:
            pass


def main() -> None:
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    snapshot = load_postos_snapshot()

    seed_hashes(redis, snapshot)
    ensure_geo(redis, snapshot)
    ensure_global_timeseries(redis)
    ensure_ranking_placeholders(redis, snapshot)
    ensure_search_index(redis)

    cleanup_keys = []
    for item in snapshot.values():
        for combustivel in COMBUSTIVEIS:
            cleanup_keys.append(f"ranking:preco:{combustivel}:{item['estado']}")
            cleanup_keys.append(f"ranking:preco:{combustivel}:{item['cidade']}")
            cleanup_keys.append(f'ranking:variacao:{combustivel}')
    cleanup_keys.extend([
        'ranking:combustivel:buscas',
        'ranking:buscas:cidade',
        'ranking:interacoes:postos',
    ])
    remove_seed_members(redis, set(cleanup_keys))

    print(f'[REDIS] Snapshot de postos carregado: {len(snapshot)} hashes posto:*')
    print('[REDIS] Índice RediSearch criado: idx:postos')
    print('[REDIS] Estrutura GEO criada: geo:postos')
    print('[REDIS] TimeSeries criadas por posto/combustível e métricas globais de busca')
    print('[REDIS] Convenções de rankings inicializadas: preço, variação, buscas e interações')


if __name__ == '__main__':
    main()
