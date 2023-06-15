import requests
import os
from base_logger import logger

def get_api_data(url: str) -> dict:
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        err_msg = f"Error {response.status_code} occurred while accessing {url}"
        logger.error(err_msg)
        raise Exception(err_msg)
        return None

if __name__ == "__main__":
    get_api_data(None)