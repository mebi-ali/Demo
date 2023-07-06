import json
import requests #type: ignore
from unittest import TestCase
from unittest.mock import MagicMock, patch

from src.matching_service.matching import get_report_category

@patch('src.matching_service.matching.requests')
class TestMatching(TestCase):

    def test_get_report_category_success(self, mock_requests):
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": 3,
            "created": "2023-05-26T00:12:05.829Z",
            "updated": "2022-11-25T08:43:22.192Z",
            "patient_id": 4524,
            "category": "Anxiety",
            "patient_form_link": "https://zesty-nonconformist.net/"
            }
        mock_requests.get.return_value = mock_response
        
        self.assertEqual(get_report_category(3), "Anxiety")
    
    
    def test_get_report_category_fail(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_requests.get.return_value=mock_response
        self.assertIsNotNone(get_report_category(3))