import requests
import json
from redis_connector import redis_client
from base_logger import logger

def get_report_id_data(report_id: int) -> str:
    url = 'https://xloop-dummy.herokuapp.com/report/' + str(report_id)
    response = requests.get(url)
    if response.status_code == 200:
        response_data = response.json()
        logger.info('Report category received')
        return response_data['category']
    else:
        err_msg = f"Error {response.status_code} occurred while accessing {url}"
        logger.error('Report ID not found')
        raise Exception(err_msg)

def matching_councillors(report_category: str=None, number_of_doctors: int=15):
    if report_category is None:
        report_category = get_report_id_data()
    councillors_with_ratings = json.loads(redis_client.get(report_category))
    top_councillors = [json.loads(item) for item in councillors_with_ratings[:number_of_doctors]]
    logger.info("Returing top councillors")
    return top_councillors

if __name__ == "__main__":
    matching_councillors()
