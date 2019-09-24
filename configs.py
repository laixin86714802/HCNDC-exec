# !/usr/bin/env python
# -*- coding:utf-8 -*-

"""
配置文件载入所有配置
"""
from conn.mysqldb import MySQLdb
from flask import Flask
import json
from flask import Markup

from server.super_config import SuperConf
from server.logger import create_logger
from server.decorators import DictModel
from scheduler.scheduler import SchedulerConfig, Scheduler

# 配置文件
config = SuperConf(path='superconf.json')

# Flask API 对象
app = Flask(__name__)
app.jinja_env.filters['json'] = lambda v: Markup(json.dumps(v))
app.secret_key = '\x1a\x8dfb#\xb9\xc8\xc3\x05\x86|\xda\x96\xff\xceo3\xf0\xa3\xb8\x8beoW'

# 调度对象
app.config.from_object(SchedulerConfig())
scheduler = Scheduler()

# 日志对象
log = create_logger(config, 'exec')

# 数据库连接
db = DictModel({
    'etl_db': MySQLdb(dict(config.mysql.etl))
})
