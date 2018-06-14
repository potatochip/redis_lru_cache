"""Redis-based LRU cache"""
# TODO: need to save the size to redis and use that unless size is set
import cPickle as pickle
import logging
from pprint import pformat

from .connection import redis_server as connection


class LRUCache(object):
    """LRU Cache that allows access across modules and servers

    Args:
        size (int): maximum size of the cache
        name (name): the name associated with the cache
        expire_size (int, optional): the number of items to remove when the
            maximum cache size is reached
    """
    def __init__(self, size, name, expire_size=200):
        self.name = name
        self.size = size
        self.expire_size = expire_size
        self._keys = 'lru:'+name+'_keys'
        self._store = 'lru:'+name+'_store'
        self._register_lua_scripts()

    def _register_lua_scripts(self):
        self.__set_item = connection.register_script("""
            if redis.call('HEXISTS', '{store}', ARGV[1]) == 0 then
                if redis.call('ZCARD', '{keys}') >= {size} then
                    -- cleanup if max size reached
                    local remove = redis.call('ZRANGE', '{keys}', 0, {expire_size})
                    redis.call('ZREMRANGEBYRANK', '{keys}', 0, {expire_size})
                    redis.call('HDEL', '{store}', unpack(remove))
                end
                redis.call('HSET', '{store}', ARGV[1], ARGV[2])
                redis.call('ZADD', '{keys}', 0, ARGV[1])
            end
        """.format(
            keys=self._keys,
            store=self._store,
            size=self.size,
            expire_size=self.expire_size
        ))
        self.__get_item = connection.register_script("""
            local result = redis.call('HGET', '{store}', ARGV[1])
            if result ~= false then
                redis.call('ZINCRBY', '{keys}', 1.0, ARGV[1])
            end
            return result
        """.format(
            keys=self._keys,
            store=self._store
        ))
        self.__batch_get = connection.register_script("""
            local results = redis.call('HMGET', '{store}', unpack(ARGV))
            for i, v in ipairs(results) do
                if v ~= false then
                    redis.call('ZINCRBY', '{keys}', 1.0, ARGV[i])
                end
            end
            return results
        """.format(
            keys=self._keys,
            store=self._store
        ))

    def __setitem__(self, key, value):
        self.__set_item(args=[key, pickle.dumps(value)])

    def __getitem__(self, key):
        result = self.__get_item(args=[key])
        if result:
            return pickle.loads(result)
        raise KeyError(key)

    def __repr__(self):
        return "<{} Redis-LRUCache object>".format(self.name)

    def __str__(self):
        return pformat({k: pickle.loads(v) for k, v in connection.hscan(self._store)[1].items()})

    def __len__(self):
        return connection.zcard(self._keys)

    def clear(self):
        """removes everything from the cache"""
        pipe = connection.pipeline()
        pipe.delete(self._keys)
        pipe.delete(self._store)
        pipe.execute()
        logging.warning('Cleared %s cache', self.name)

    def get(self, key, default=None):
        """default item getter

        Args:
            key (string): the key for the item in the cache
            default (obj): the value to return if the key does not exist in the cache

        Return:
            obj: the object stored in the cache
        """
        try:
            return self[key]
        except KeyError:
            return default

    def batch_get(self, keys, default=None):
        """default batch item getter

        Args:
            keys (list of string): the keys for the item in the cache
            default (obj): the value to return if the key does not exist in the cache

        Return:
            iter of obj: the objects stored in the cache
        """
        for i in self.__batch_get(args=keys):
            if not i:
                yield default
            else:
                yield pickle.loads(i)
