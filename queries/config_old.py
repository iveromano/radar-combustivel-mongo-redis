import os
from dotenv import load_dotenv

load_dotenv('.env.local')
load_dotenv()

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/?replicaSet=rs0&directConnection=true')
MONGO_DB = os.getenv('MONGO_DB', 'radar_combustivel')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))
POLL_INTERVAL_SECONDS = float(os.getenv('POLL_INTERVAL_SECONDS', '2'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
