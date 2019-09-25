# !/usr/bin/python
# -*- coding: utf-8 -*-
import pymysql

from configs import log


class MysqlLock(object):
    """mysql分布式锁"""

    def __init__(self, opts, lock_name='scheduler_lock', timeout=30):
        self.conn = pymysql.connect(
            host=opts['host'],
            port=opts['port'],
            user=opts['user'],
            passwd=opts['password'],
            db=opts.get('database', ''),
            charset='utf8',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor
        )
        # 锁名
        self.lock_name = lock_name
        # 超时时间
        self.timeout = timeout

    def execute(self, sql, args=None):
        """执行"""
        cursor = self.conn.cursor()
        if args and isinstance(args, list):
            cursor.executemany(sql, args)
        else:
            cursor.execute(sql, args)
        return cursor

    def __enter__(self):
        """获取锁"""
        try:
            # 获取锁
            sql = "SELECT GET_LOCK('%s', %s) AS 'lock'" % (self.lock_name, self.timeout)
            cursor = self.execute(sql)
            if cursor.rowcount != 1:
                raise BaseException('锁[%s]在lock函数中返回多行.')
            result = cursor.fetchone()
            if result['lock'] == 0:
                raise BaseException('锁[%s]已被其他客户端锁定.' % self.lock_name)
            elif result['lock'] == 1:
                log.debug('获取锁[%s]成功.' % self.lock_name)
                return
            else:
                raise BaseException('mysql lock函数其他异常.')
        except Exception as e:
            log.error(e, exc_info=True)
            raise e

    def __exit__(self, exc_type, exc_value, traceback):
        """释放锁"""
        try:
            sql = "SELECT RELEASE_LOCK('%s') AS 'release_lock'" % self.lock_name
            cursor = self.execute(sql)
            result = cursor.fetchone()
            if result['release_lock'] == 0:
                raise BaseException('锁[%s]未被释放, 该锁并非由此数据库连接创建.' % self.lock_name)
            elif result['release_lock'] == 1:
                log.debug("锁[%s]释放成功." % self.lock_name)
            else:
                raise BaseException("锁[%s]不存在." % self.lock_name)
        except Exception as e:
            log.error(e, exc_info=True)
            raise e
        finally:
            # 一定要释放连接, 不然会死锁
            self.conn and self.conn.close()


if __name__ == '__main__':
    opts = {
        "host": "localhost",
        "port": 3306,
        "db": "hcndc",
        "user": "root",
        "passwd": "123456",
        "maxConnections": 10
    }

    client = MysqlLock(opts)
