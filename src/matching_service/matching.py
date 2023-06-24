import os
import requests # type: ignore
import json
from redis_connector import get_redis_client
from base_logger import logger
from dotenv import load_dotenv
load_dotenv()

def get_report_id_data(report_id: int) -> str:
    """
    Retrieve the category of a report with the given report_id.

    Parameters:
    - report_id (int): The ID of the report to retrieve the category for.

    Returns:
    - str: The category of the report.
    """
    url = f"{os.getenv('BASE_URL')}/report/{report_id}"
    response = requests.get(url)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        err_msg = f"Error {response.status_code} occurred while accessing {url}"
        logger.error(err_msg)
        raise
    response_data = response.json()
    return response_data['category']

def matching_councillors(report_id: int, number_of_councillors: int=15):
    """
    Retrieve the top councillors matching the given report_id and number_of_councillors.

    Parameters:
    - report_id (int): The ID of the report to retrieve councillors for.
    - number_of_councillors (int, optional): The number of councillors to match.
        Defaults to 15 if not provided.

    Returns:
    - list: A list of dictionaries representing the top councillors.
    """
    report_category = get_report_id_data(report_id)
    councillors_with_ratings = json.loads(get_redis_client().get(report_category).decode())
    top_councillors = [json.loads(item) for item in councillors_with_ratings[:number_of_councillors]]
    logger.info("Returning top councillors")
    return top_councillors

if __name__ == "__main__":
    matching_councillors()
