import redis
import requests
import json
import logging
from fastapi import FastAPI
from matching import match_councillors

app = FastAPI()


@app.post("/councillors/")

def get_councillors():

    data = match_councillors()
    return data



# if __name__ == "__main__":
#     get_report_data()







# df = json.loads(redis_client.get('data'))

# filtered_data = [record for record in df if record['specialization'] == 'Depression']
# filtered_data = json.dumps(filtered_data, indent=4) 

# print(filtered_data)
