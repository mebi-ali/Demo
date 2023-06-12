import redis
import json
from transform import joined_data

# Connect to Redis
redis_host = 'localhost'
redis_port = 6379
redis_db = 0
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)


def load_data_to_radis():
    rows = data.collect()
    data_dict = [row.asDict() for row in rows]
    json_data = json.dumps(data_dict, indent=4)
    redis_client.set('data', json_data)
    return


if __name__ == "__main__":
    data = joined_data()
    load_data_to_radis()



#### Matching service
    # df = json.loads(redis_client.get('data'))

    # filtered_data = [record for record in df if record['specialization'] == 'Depression']
    # filtered_data = json.dumps(filtered_data, indent=4) 

    # print(filtered_data)



##### Query
# query = f"JSON.GET data . specialization == 'Depression'"

# result = redis_client.execute_command('JSON.QUERY', 'data', '.', query)

