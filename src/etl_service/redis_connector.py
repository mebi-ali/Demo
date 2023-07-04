import redis  # type: ignore


def get_redis_client() -> redis.client.Redis:
<<<<<<< HEAD
    redis_client = redis.Redis(host="redis", port=6379, db=0)
=======
    redis_client = redis.Redis(host="localhost", port=6379, db=0)
>>>>>>> f1cddabe60ee508a6e5c931f110b118b29f0ebdb
    return redis_client
