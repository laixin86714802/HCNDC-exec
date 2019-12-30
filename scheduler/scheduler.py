# !/usr/bin/env python
# -*- coding: utf-8 -*-


from flask_apscheduler import APScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from server.super_config import SuperConf
# 配置文件
config = SuperConf(path='superconf.json')


class Scheduler(APScheduler):
    """单例模式"""

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance


class SchedulerConfig(object):
    """调度配置"""
    # 数据库持久化
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(
            url='mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8&autocommit=true' % {
                'user': config.mysql.etl['user'],
                'password': config.mysql.etl['password'],
                'host': config.mysql.etl['host'],
                'port': config.mysql.etl['port'],
                'database': config.mysql.etl['database']
            }, engine_options={
                'max_overflow': config.schedule.max_overflow,
                'pool_size': 5,
                'pool_recycle': 30
            }, tablename=config.exec.table_name)
    }
    # api开关
    SCHEDULER_API_ENABLED = True
    # 支持中文
    JSON_AS_ASCII = False
    ENCODING = 'utf8'
    DEBUG = True
    THREADED = True
    # 最大并行线程数
    SCHEDULER_EXECUTORS = {
        'default': {'type': 'threadpool', 'max_workers': config.schedule.max_workers}
    }
    # 调度任务实例数
    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': True,
        'max_instances': config.schedule.max_instances,
        'misfire_grace_time': config.schedule.misfire_grace_time
    }



