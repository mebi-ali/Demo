import redis 

redis_host = '0.0.0.0' 
redis_port = 6379
redis_db = 0
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)