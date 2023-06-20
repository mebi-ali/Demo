import redis
import json
from base_logger import logger
from transform import transformations 
from redis_connector import redis_client

def load_data_to_radis(specializations_dfs: dict) -> dict:
    for key, val in specializations_dfs.items():
        json_data = [json.loads(row) for row in val]
        redis_client.set(key, json.dumps(json_data, indent=2))
    logger.info('Data Stored in Redis.')
    return specializations_dfs

if __name__ == "__main__":
    load_data_to_radis(transformations())