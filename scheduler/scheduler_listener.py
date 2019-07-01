# !/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from configs import db, log, config
from model.scheduler import SchedulerModel
from util.request_package import request
from conn.mongo import MongoLock

def listener(event):
    """监听器"""
    # 获取执行id, 任务id
    [(exec_id, job_id)] = re.findall('exec_(\\d+)_(\\d+)', event.job_id)
    # 异常
    if event.exception:
        log.warn('执行完毕, 执行id: %s, 任务id: %s, 任务状态: %s' % (exec_id, job_id, 'failed'))
        # 修改数据库, 分布式锁
        with MongoLock(config.mongo, 'scheduler_lock'):
            SchedulerModel.update_exec_job_status(db.etl_db, exec_id, job_id, 'failed')
            # 回调web服务
            result = request(exec_id, 'failed')
            if not result:
                log.error('回调web服务失败, 执行id: %s, 任务id: %s, 任务状态: %s' % (exec_id, job_id, 'failed'))
    # 正常
    else:
        log.info('执行完毕, 执行id: %s, 任务id: %s, 任务状态: %s' % (exec_id, job_id, 'succeeded'))
        # 修改数据库, 分布式锁
        with MongoLock(config.mongo, 'scheduler_lock'):
            SchedulerModel.update_exec_job_status(db.etl_db, exec_id, job_id, 'succeeded')
            # 回调web服务
            result = request(exec_id, 'succeeded')
            if not result:
                log.error('回调web服务失败, 执行id: %s, 任务id: %s, 任务状态: %s' % (exec_id, job_id, 'succeeded'))
