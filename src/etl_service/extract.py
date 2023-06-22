import os
import requests  # type: ignore
from base_logger import logger
# from dotenv import load_dotenv
# load_dotenv()

def get_api_data(url: str):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        err_msg = f"Error {response.status_code} occurred while accessing {url}"
        logger.error(err_msg)
        response.raise_for_status()

# urls = {
    # "appointment": f"{os.getenv("BASE_URL)}/appointment",
    # "councillor": f"{os.getenv('BASE_URL')}/councillor",
    # "patient_councillor": f"{os.getenv('BASE_URL')}/patient_councillor",
    # "rating": f"{os.getenv('BASE_URL')}/rating",
# }

# urls = {
#     "appointment": env_variables['BASE_URL'] + "/appointment",
#     "councillor": env_variables['BASE_URL'] + "/councillor",
#     "patient_councillor": env_variables['BASE_URL'] + "/patient_councillor",
#     "rating": env_variables['BASE_URL'] + "/rating"
# }

if __name__ == "__main__":
    print("**************************************8",os.getenv("BASE_URL"))
    # urls = {key: get_api_data(val) for key, val in urls.items()}
