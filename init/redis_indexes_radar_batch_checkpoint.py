import os
import json
from typing import Dict, List, Tuple

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
BLOCK_SIZE = int(os.getenv('BLOCK_SIZE', '10000'))
CHECKPOINT_FILE = os.getenv('CHECKPOINT_FILE', 'redis_indexes_checkpoint.json')


def normalize(value: str) -> str:
    return str(value or '').strip().lower().replace(' ', '_')


def print_block(title: str) -> None:
    print('\n' + '=' * 80)
    print(title)
    print('=' * 80)


def load_checkpoint() -> int:
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return int(data.get('next_index', 0))
    except Exception:
        return 0


def save_checkpoint(next_index: int, last_posto_id: str | None) -> None:
    data = {'next_index': next_index, 'last_posto_id': last_posto_id}
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def create_timeseries_safe(redis: Redis, key: str, posto_id: str, combustivel: str) -> None:
    try:
        redis.execute_command(
            'TS.CREATE',
            key,
            'RETENTION', RETENTION_MS,
            'DUPLICATE_POLICY', 'LAST',
            'LABELS',
            'posto_id', posto_id,
            'combustivel', combustivel,
            'metric', 'preco',
        )
    except Exception as exc:
        if 'already exists' not in str(exc).lower():
            raise


def load_postos_snapshot() -> List[Tuple[str, dict]]:
    print_block('Carregando snapshot do MongoDB')
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    db = mongo[DB_NAME]
    postos = db[POSTOS]
    localizacoes = db[LOCALIZACOES]

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

    ordered = list(snapshot.items())
    ordered.sort(key=lambda x: x[0])
    print(f'[REDIS] Snapshot carregado: {len(ordered)} postos.')
    return ordered


def create_search_index(redis: Redis) -> None:
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


def seed_block(redis: Redis, items: List[Tuple[str, dict]], start_idx: int, end_idx: int) -> None:
    print_block(f'Processando postos {start_idx + 1}-{end_idx}')
    pipe = redis.pipeline(transaction=False)
    count = 0
    last_id = None

    for global_idx in range(start_idx, end_idx):
        posto_id, item = items[global_idx]
        last_id = posto_id
        pipe.hset(f'posto:{posto_id}', mapping=item)
        for combustivel in COMBUSTIVEIS:
            create_timeseries_safe(redis, f'ts:posto:{posto_id}:{combustivel}', posto_id, combustivel)
        lon = item['longitude']
        lat = item['latitude']
        if lon != 0 or lat != 0:
            pipe.execute_command('GEOADD', 'geo:postos', lon, lat, posto_id)
        count += 1

    pipe.execute()
    save_checkpoint(end_idx, last_id)
    print(f'[REDIS] Bloco concluído: {start_idx + 1}-{end_idx} ({count} postos).')


def seed_all(redis: Redis, items: List[Tuple[str, dict]]) -> None:
    total = len(items)
    start = load_checkpoint()
    if start > total:
        start = 0
    print(f'[REDIS] Retomando do índice {start + 1 if start < total else total}.')

    while start < total:
        end = min(start + BLOCK_SIZE, total)
        seed_block(redis, items, start, end)
        start = end


def ensure_global_timeseries(redis: Redis) -> None:
    print_block('Criando séries globais')
    for key, labels in [
        ('ts:buscas:latencia_ms', ('metric', 'latencia_ms', 'scope', 'buscas')),
        ('ts:buscas:resultado_count', ('metric', 'resultado_count', 'scope', 'buscas')),
    ]:
        try:
            redis.execute_command('TS.CREATE', key, 'RETENTION', RETENTION_MS, 'DUPLICATE_POLICY', 'LAST', 'LABELS', *labels)
        except Exception as exc:
            if 'already exists' not in str(exc).lower():
                raise


def init_rankings(redis: Redis, items: List[Tuple[str, dict]]) -> None:
    print_block('Inicializando rankings')
    ufs = sorted({item['estado'] for _, item in items if item['estado']})
    cidades = sorted({item['cidade'] for _, item in items if item['cidade']})
    for uf in ufs:
        for combustivel in COMBUSTIVEIS:
            redis.zadd(f'ranking:preco:{combustivel}:{uf}', {'__seed__': 999999.0}, nx=True)
    for cidade in cidades:
        for combustivel in COMBUSTIVEIS:
            redis.zadd(f'ranking:preco:{combustivel}:{cidade}', {'__seed__': 999999.0}, nx=True)
    for combustivel in COMBUSTIVEIS:
        redis.zadd(f'ranking:variacao:{combustivel}', {'__seed__': 0.0}, nx=True)
    redis.zadd('ranking:combustivel:buscas', {'__seed__': 0.0}, nx=True)
    redis.zadd('ranking:buscas:cidade', {'__seed__': 0.0}, nx=True)
    redis.zadd('ranking:interacoes:postos', {'__seed__': 0.0}, nx=True)


def cleanup_rankings(redis: Redis, items: List[Tuple[str, dict]]) -> None:
    print_block('Removendo seeds temporários')
    ufs = sorted({item['estado'] for _, item in items if item['estado']})
    cidades = sorted({item['cidade'] for _, item in items if item['cidade']})
    keys = [
        *[f'ranking:preco:{combustivel}:{uf}' for uf in ufs for combustivel in COMBUSTIVEIS],
        *[f'ranking:preco:{combustivel}:{cidade}' for cidade in cidades for combustivel in COMBUSTIVEIS],
        *[f'ranking:variacao:{combustivel}' for combustivel in COMBUSTIVEIS],
        'ranking:combustivel:buscas',
        'ranking:buscas:cidade',
        'ranking:interacoes:postos',
    ]
    for key in keys:
        try:
            redis.zrem(key, '__seed__')
        except Exception:
            pass


def main() -> None:
    print_block('Iniciando bootstrap do Redis para Radar Combustível')
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    items = load_postos_snapshot()
    seed_all(redis, items)
    ensure_global_timeseries(redis)
    init_rankings(redis, items)
    create_search_index(redis)
    cleanup_rankings(redis, items)
    print_block('Finalização')
    print(f'[REDIS] Bootstrap concluído. Checkpoint em {CHECKPOINT_FILE}.')


if __name__ == '__main__':
    main()

