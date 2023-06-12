import requests
import os
import sys

sys.path.insert(0, '.')
from etl_service.load import redis_host

print(redis_host)