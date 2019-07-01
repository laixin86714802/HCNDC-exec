# -*- coding: utf-8 -*-

import pymongo
import time
from datetime import datetime, timedelta


class MongoLinks(object):
    """mongo连接类"""

    def __init__(self, config, collection):
        self.host = config['host']
        self.port = config['port']
        self.db = config['db']
        self.collection_name = collection
        self.user = config.get('user', '')
        self.password = config.get('password', '')
        self.conn = pymongo.MongoClient(host=self.host, port=self.port)

        if self.user and self.password:
            db_auth = self.conn[self.db]
            db_auth.authenticate(self.user, self.password)
            self.collection = self.conn[self.db][self.collection_name]
        else:
            self.collection = self.conn[self.db][self.collection_name]


class MongoLock(object):
    """mongo分布式锁"""

    def __init__(self, config, collection, key='lock', expire=60, timeout=30):
        self.host = config['host']
        self.port = config['port']
        self.db = config['db']
        self.collection_name = collection
        self.user = config.get('user', '')
        self.password = config.get('password', '')
        self.conn = pymongo.MongoClient(host=self.host, port=self.port)

        if self.user and self.password:
            db_auth = self.conn[self.db]
            db_auth.authenticate(self.user, self.password)
            self.collection = self.conn[self.db][self.collection_name]
        else:
            self.collection = self.conn[self.db][self.collection_name]

        self.key = key
        self.expire = expire
        self.timeout = timeout

    def __enter__(self):
        timeout = self.timeout
        while timeout >= 0:
            lock = list(self.collection.find({'key': self.key}).limit(1))
            # 锁是否被获得
            if lock and lock[0]['export'] + timedelta(hours=8) > datetime.now():
                timeout -= 1
                time.sleep(1)
                continue
            # 自增
            result = self.collection.find_and_modify(
                query={'key': self.key},
                update={'$inc': {'value': 1}, '$set': {'export': datetime.now() + timedelta(hours=-8, seconds=self.expire)}},
                upsert=True,
                new=True
            )
            # 获得锁
            if result['value'] == 1:
                return
            else:
                timeout -= 1
                time.sleep(1)
                continue
        # 重试用尽, 异常
        raise BaseException("获取锁时间超时")

    def __exit__(self, exc_type, exc_value, traceback):
        self.collection.remove({'key': self.key})
        self.conn.close()
