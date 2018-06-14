"""shared connection to redis"""
import redis

redis_settings = {
    'host': 'localhost',
    'port': 6379,
    'db': 2
}

_pool = redis.ConnectionPool(**redis_settings)

redis_server = redis.Redis(connection_pool=_pool)
