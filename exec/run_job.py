# !/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import time

from model.scheduler import SchedulerModel
from configs import db, log


def start_job(exec_id, interface_id, job_id, server_dir, server_script, return_code, params, status):
    """开始任务"""
    log.info('开始任务: %s' % str({
        'exec_id': exec_id,
        'interface_id': interface_id,
        'job_id': job_id,
        'server_dir': server_dir,
        'server_script': server_script,
        'return_code': return_code,
        'params': params,
        'status': status
    }))
    if status == 'preparing':
        exec_job(exec_id, interface_id, job_id, server_dir, server_script, return_code, params)
    elif status == 'ready':
        while True:
            # 查询状态
            job = SchedulerModel.get_exec_job(db.etl_db, exec_id, interface_id, job_id)
            if not job:
                return
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
            elif current_status == 'preparing':
                log.info('任务开始执行: %s' % str({
                    'exec_id': exec_id,
                    'interface_id': interface_id,
                    'job_id': job_id,
                    'server_dir': server_dir,
                    'server_script': server_script
                }))
                exec_job(exec_id, interface_id, job_id, server_dir, server_script, return_code, params)
                break
            else:
                break


def exec_job(exec_id, interface_id, job_id, server_dir, server_script, return_code, params, retry=0):
    """执行任务"""
    # 配置参数
    params_str = ' '.join(params) if ' '.join(params).startswith(' ') else ' ' + ' '.join(params)
    server_script_run = server_script + params_str
    # 文本日志
    now_time = time.strftime('%Y-%m-%d_%H%M%S', time.localtime())
    file_name = './logs/%s_%s_%s_%s.log' % (exec_id, interface_id, job_id, now_time)
    fw = open(file_name, 'w')
    # 添加执行任务开始日志
    SchedulerModel.add_exec_detail_job(db.etl_db, exec_id, interface_id, job_id, 'INFO', server_dir,
                                       server_script_run, '任务开始', 1)
    # 子进程
    p = subprocess.Popen(
        server_script_run,
        cwd=server_dir,
        stdout=fw,
        stderr=fw,
        shell=True,
        bufsize=0
    )
    # 执行任务开始
    SchedulerModel.exec_job_start(db.etl_db, exec_id, interface_id, job_id, p.pid)
    # 获取返回码
    ret_code = p.wait()
    for message in open(file_name, 'r'):
        message = message.rstrip()
        if message:
            log.debug('任务详情日志: [%s]' % message)
            # 添加执行任务详情日志
            SchedulerModel.add_exec_detail_job(db.etl_db, exec_id, interface_id, job_id, 'INFO', server_dir,
                                               server_script_run, message, 2)
    # 结束
    if ret_code is not None:
        # 添加执行任务结束日志
        SchedulerModel.add_exec_detail_job(db.etl_db, exec_id, interface_id, job_id, 'INFO', server_dir,
                                           server_script_run, '任务结束', 3)
        # 异常
        if ret_code != return_code:
            if retry >= 3:
                raise Exception('任务异常')
            # 重试三次
            else:
                time.sleep(10)
                exec_job(exec_id, interface_id, job_id, server_dir, server_script, return_code, params, retry=retry + 1)
        return
