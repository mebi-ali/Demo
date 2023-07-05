import os
import requests #type: ignore
import json

from unittest import TestCase
from unittest.mock import MagicMock, patch
from src.etl_service import extract
# from src.etl_service import base_logger
from dotenv import load_dotenv
load_dotenv()

@patch("requests.get")
class TestExtract(TestCase):
    # def test_get_api_data_200_response(self, mock_request.get):
    #     return_json = {
    #         "test_key": "test_value"
    #     }
        
    #     # mock the response 
    #     response = requests.Response()
    #     response.status_code = 200
    #     response._content = json.dumps().encode("utf-8")
    #     endpoint = "appointment"
    #     extract.get_api_data(f"{os.getenv('BASE_URL')}/{endpoint}")

    #     mock_request_get
    
    def test_get_api_data_fail(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_requests.get.return_value = mock_response
        self.assertIsNone(extract.get_api_data("https://xloop-dummy.herokuapp.com/appointment")) 