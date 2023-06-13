import requests
import json
from redis_connector import redis_client 


def get_report_id(id=0):
    url = 'https://xloop-dummy.herokuapp.com/report/' + str(id)
    response = requests.get(url) 
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code} occurred while accessing {url}")
        return None
    
    
def match_councillors():

    report_id = get_report_id()
    report_id = report_id['category']
    print(report_id)
    redis_client.get('data')
    df = json.loads(redis_client.get('data'))

    councillors = [record for record in df if record['specialization'] == report_id]
    councillors = json.dumps(councillors, indent=4) 
    
    return json.loads(councillors)
