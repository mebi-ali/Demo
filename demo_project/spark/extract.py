import requests
import os
import sys
sys.path.insert(0, '.')
from all_apis import get_api_data, urls

data = {} # will decide at the end where to put data veriable

def get_data():
    # data = {}
    for key, val in urls.items():
        data[key] = get_api_data(val)
    return data

def main():
    return get_data()

if __name__ == "__main__":
    main()




