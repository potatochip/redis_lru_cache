import multiprocessing as mp

import numpy as np
import pytest

from .connection import redis_server as r
from .lru_cache import LRUCache

CACHE = LRUCache(10, 'test_cache')


def setup_function():
    CACHE.clear()


def teardown_module():
    CACHE.clear()


def mp_func(i):
    results = CACHE.get(i)
    if not results:
        results = 'something'
        CACHE[i] = results
    check_key_store_in_sync()
    return results


def check_key_store_in_sync():
    pipe = r.pipeline()
    pipe.hlen(CACHE._store)
    pipe.zcard(CACHE._keys)
    results = pipe.execute()
    assert len(set(results)) == 1


def test_multiprocess_in_sync():
    items = np.random.choice(range(100), 1000)
    pool = mp.Pool(mp.cpu_count())
    pool.map(mp_func, items)
    pool.close()
    pool.join()


def test_add():
    CACHE['hey'] = 'thing'
    assert CACHE['hey'] == 'thing'


def test_batch_get():
    keys = range(10)
    for i in keys:
        CACHE[i] = 'thing'
    results = CACHE.batch_get(keys)
    assert list(results) == ['thing']*len(keys)


def test_add_complex_object():
    CACHE['hey'] = {'my': 'thing'}
    assert CACHE['hey'] == {'my': 'thing'}


def test_expire():
    CACHE['expiring'] = 'thing'
    keys = range(10)
    for i in keys:
        CACHE[i] = 'thing'
    assert CACHE.get('EXPIRING') == None


def test_get_default():
    CACHE['object'] = 'thing'
    assert CACHE.get('object') == 'thing'
    assert CACHE.get('not_object', 'nobody home') == 'nobody home'


def test_get_not_exists():
    with pytest.raises(KeyError):
        CACHE['not_object']


def test_batch_get_default():
    keys = range(10)
    for i in keys:
        CACHE[i] = 'thing'
    results = CACHE.batch_get(keys+[11], 'nobody home')
    assert list(results) == ['thing']*len(keys) + ['nobody home']
