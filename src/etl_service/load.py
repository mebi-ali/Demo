import redis
import json
from base_logger import logger
from transform import joined_data
from redis_connector import redis_client

def load_data_to_radis(specializations_dfs: dict) -> None:
    for key, val in specializations_dfs.items():
        redis_client.set(key, json.dumps(val, indent=4))
    logger.info('Data Stored in Redis')
    return None

if __name__ == "__main__":
    load_data_to_radis(joined_data())