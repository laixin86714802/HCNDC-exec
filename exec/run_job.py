# !/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import time

from model.scheduler import SchedulerModel
from configs import db, log


def start_job(exec_id, job_id, server_dir, server_script, return_code, params, status):
    """开始任务"""
    log.info('开始任务: %s' % str({
        'exec_id': exec_id,
        'job_id': job_id,
        'server_dir': server_dir,
        'server_script': server_script,
        'return_code': return_code,
        'params': params,
        'status': status
    }))
    if status == 'preparing':
        exec_job(exec_id, job_id, server_dir, server_script, return_code, params)
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
                exec_job(exec_id, job_id, server_dir, server_script, return_code, params)


def exec_job(exec_id, job_id, server_dir, server_script, return_code, params):
    """执行任务"""
    # 配置参数
    params_str = ' '.join(params) if ' '.join(params).startswith(' ') else ' ' + ' '.join(params)
    server_script = server_script + params_str
    # 文本日志
    file_name = './logs/%s_%s_%s.log' % (exec_id, job_id, time.strftime('%Y-%m-%d_%H%M%S', time.localtime()))
    fw = open(file_name, 'w')
    # 添加执行任务开始日志
    SchedulerModel.add_exec_detail_job(db.etl_db, exec_id, job_id, 'INFO', server_dir, server_script, '任务开始', 1)
    # 子进程
    p = subprocess.Popen(
        server_script,
        cwd=server_dir,
        stdout=fw,
        stderr=fw,
        shell=True,
        bufsize=0
    )
    # 执行任务开始
    SchedulerModel.exec_job_start(db.etl_db, exec_id, job_id, p.pid)
    # 获取返回码
    ret_code = p.wait()
    for message in open(file_name, 'r'):
        message = message.rstrip()
        if message:
            log.debug('任务详情日志: [%s]' % message)
            # 添加执行任务详情日志
            SchedulerModel.add_exec_detail_job(db.etl_db, exec_id, job_id, 'INFO', server_dir, server_script, message,
                                               2)
    # 结束
    if ret_code is not None:
        # 添加执行任务结束日志
        SchedulerModel.add_exec_detail_job(db.etl_db, exec_id, job_id, 'INFO', server_dir, server_script,
                                           '任务结束', 3)
        # 异常
        if ret_code != return_code:
            raise Exception('任务异常')
        return
