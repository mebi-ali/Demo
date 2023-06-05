import requests
import os
import sys
sys.path.insert(0, '.')
from fast.main import get_api_data, urls

data = {}

def get_data():
    
    for key, val in urls.items():
        data[key] = get_api_data(val)
    return data

def send_data():
    if(len(data) == 0):
        return get_data()
    return data


if __name__ == "__main__":
    send_data()



# keys = rating[0].keys()
# print(keys)

# df = spark.createDataFrame(appointment, schema=keys)
# # Show the DataFrame
# df.show()


