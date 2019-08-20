# !/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import gevent
import time

from model.scheduler import SchedulerModel
from configs import db, log, config
from conn.mongo import MongoLinks
from util.time_local import local_to_utc


def start_job(exec_id, job_id, server_dir, server_script, status):
    """开始任务"""
    log.info('开始任务: %s' % str({
        'exec_id': exec_id,
        'job_id': job_id,
        'server_dir': server_dir,
        'server_script': server_script
    }))
    if status == 'preparing':
        exec_job(exec_id, job_id, server_dir, server_script)
    else:
        while True:
            # 查询状态
            job = SchedulerModel.get_exec_job(db.etl_db, exec_id, job_id)
            # 依赖任务状态
            current_status = 'preparing'
            for prep_id in job['in_degree']:
                prep_job = SchedulerModel.get_exec_prep_job(db.etl_db, prep_id)
                if job['run_period'] and prep_job['run_period'] and job['run_period'] > prep_job['run_period']:
                    current_status = 'ready'
                    break
            # 等待依赖任务完成
            if current_status == 'ready':
                log.info('等待依赖任务完成: %s' % str({
                    'exec_id': exec_id,
                    'job_id': job_id,
                    'server_dir': server_dir,
                    'server_script': server_script
                }))
                time.sleep(20)
            else:
                log.info('任务开始执行: %s' % str({
                    'exec_id': exec_id,
                    'job_id': job_id,
                    'server_dir': server_dir,
                    'server_script': server_script
                }))
                exec_job(exec_id, job_id, server_dir, server_script)


def exec_job(exec_id, job_id, server_dir, server_script):
    """执行任务"""
    # 执行任务开始
    SchedulerModel.exec_job_start(db.etl_db, exec_id, job_id)
    # 日志对象
    mongo = MongoLinks(config.mongo, 'job_logs')
    mongo.collection.insert({
        'exec_id': exec_id,
        'job_id': job_id,
        'level': 'INFO',
        'server_dir': server_dir,
        'server_script': server_script,
        'message': '任务开始',
        'type': 1,
        'time': local_to_utc()
    })
    # 子进程, shell参数windows下False, linux下True
    p = subprocess.Popen(
        server_script,
        cwd=server_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False
    )
    while True:
        # 正常日志
        for line in iter(p.stdout.readline, b''):
            message = line.decode('utf-8', 'ignore').replace('\n', '').replace('\r', '')
            gevent.sleep(0)
            if message:
                mongo.collection.insert({
                    'exec_id': exec_id,
                    'job_id': job_id,
                    'level': 'INFO',
                    'server_dir': server_dir,
                    'server_script': server_script,
                    'message': message,
                    'type': 2,
                    'time': local_to_utc()
                })
        # 异常日志
        for line in iter(p.stderr.readline, b''):
            message = line.decode('utf-8', 'ignore').replace('\n', '').replace('\r', '')
            gevent.sleep(0)
            mongo.collection.insert({
                'exec_id': exec_id,
                'job_id': job_id,
                'level': 'ERROR',
                'server_dir': server_dir,
                'server_script': server_script,
                'message': message,
                'type': 2,
                'time': local_to_utc()
            })
        # 结束
        retcode = p.poll()
        if retcode is not None:
            mongo.collection.insert({
                'exec_id': exec_id,
                'job_id': job_id,
                'level': 'INFO',
                'server_dir': server_dir,
                'server_script': server_script,
                'message': '任务结束',
                'type': 3,
                'time': local_to_utc()
            })
            if retcode:
                raise Exception('任务异常')
            return
