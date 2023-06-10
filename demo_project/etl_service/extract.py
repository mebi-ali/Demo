import requests
import os
import sys
sys.path.insert(0, '.')
from all_apis import get_api_data, urls

# will decide at the end where to put data veriable

def get_data():
    return {key:get_api_data(val) for key, val in urls.items()}

if __name__ == "__main__":
    get_data()




