import requests
from base_logger import logger

def get_api_data(url: str) -> dict:
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        err_msg = f"Error {response.status_code} occurred while accessing {url}"
        logger.error(err_msg)
        raise Exception(err_msg)

urls = {
    "appointment": "https://xloop-dummy.herokuapp.com/appointment",
    "councillor": "https://xloop-dummy.herokuapp.com/councillor",
    "patient_councillor": "https://xloop-dummy.herokuapp.com/patient_councillor",
    "rating": "https://xloop-dummy.herokuapp.com/rating"
}


if __name__ == "__main__":
    get_api_data(" ")