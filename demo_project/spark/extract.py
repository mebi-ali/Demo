import requests
import os
import sys
sys.path.insert(0, '.')
from all_apis import get_api_data, urls

# will decide at the end where to put data veriable

def get_data():
    data = {key:get_api_data(val) for key, val in urls.items()}
    return data

if __name__ == "__main__":
    get_data()




