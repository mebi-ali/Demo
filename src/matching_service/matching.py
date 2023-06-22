import os
import requests # type: ignore
import json
from redis_connector import get_redis_client
from base_logger import logger
from dotenv import load_dotenv
load_dotenv()

def get_report_id_data(report_id: int) -> str:
    # url = f"{os.getenv('BASE_URL')}/report/{report_id}"
    url = f"https://xloop-dummy.herokuapp.com/report/{report_id}"
    response = requests.get(url)
    if response.status_code == 200:
        response_data = response.json()
        logger.info('Report category received')
        return response_data['category']
    else:
        err_msg = f"Error {response.status_code} occurred while accessing {url}"
        logger.error(err_msg)
        response.raise_for_status()

def matching_councillors(report_id: int, number_of_doctors: int=15):
    report_category = get_report_id_data(report_id)
    councillors_with_ratings = json.loads(get_redis_client().get(report_category).decode())
    top_councillors = [json.loads(item) for item in councillors_with_ratings[:number_of_doctors]]
    logger.info("Returning top councillors")
    return top_councillors

if __name__ == "__main__":
    matching_councillors()
