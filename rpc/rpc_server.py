# !/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import signal
import rpyc
import psutil
import datetime
import platform
import json

from exec.run_job import start_job
from configs import log, scheduler, db, config
from model.scheduler import SchedulerModel
from conn.mysql_lock import MysqlLock


class RPCServer(rpyc.Service):
    """调度服务器端"""

    @staticmethod
    def exposed_execute(exec_id, job_id, server_dir, server_script, return_code, params, status):
        """接受任务"""
        # 添加至调度器
        run_id = 'exec_%s_%s' % (exec_id, job_id)
        kwargs = {
            'exec_id': exec_id,
            'job_id': job_id,
            'server_dir': server_dir,
            'server_script': server_script,
            'return_code': return_code,
            'params': params,
            'status': status
        }
        log.info('接收任务: %s, run_id: %s' % (str(kwargs), run_id))
        # 添加调度任务
        next_run_time = datetime.datetime.now() + datetime.timedelta(seconds=2)
        try:
            # 查询任务是否存在
            run_job = SchedulerModel.get_scheduler_by_id(db.etl_db, run_id, config.exec.table_name)
            if run_job:
                return {'status': False, 'msg': '该任务已在运行中'}
            # scheduler.add_job(id=run_id, func=start_job,
            #                  args=(exec_id, job_id, server_dir, server_script, status), trigger='interval', seconds=3)
            scheduler.add_job(id=run_id, func=start_job,
                              args=(exec_id, job_id, server_dir, server_script, return_code, params, status),
                              next_run_time=next_run_time)
            return {'status': True, 'msg': '任务已开始运行'}
        except Exception as e:
            return {'status': False, 'msg': e}

    @staticmethod
    def exposed_test():
        """测试连接"""
        # CPU逻辑内核数
        cpu_count = psutil.cpu_count()
        # 系统版本
        system = platform.platform()
        # 磁盘使用
        disk_result = {
            'total': 0,
            'used': 0,
            'free': 0
        }
        disk_total = [psutil.disk_usage(i.mountpoint) for i in psutil.disk_partitions()]
        for item in disk_total:
            disk_result['total'] += item.total
            disk_result['used'] += item.used
            disk_result['free'] += item.free

        disk_result['total'] = '%0.2fGB' % (disk_result['total'] / 2 ** 30)
        disk_result['used'] = '%0.2fGB' % (disk_result['used'] / 2 ** 30)
        disk_result['free'] = '%0.2fGB' % (disk_result['free'] / 2 ** 30)
        # 内存使用
        memory_total = psutil.virtual_memory()
        memory_result = {
            'total': '%0.2fGB' % (memory_total.total / 2 ** 30),
            'used': '%0.2fGB' % (memory_total.used / 2 ** 30),
            'free': '%0.2fGB' % (memory_total.free / 2 ** 30)
        }
        log.info('获取测试连接请求')
        return json.dumps({'cpu': cpu_count, 'system': system, 'disk': disk_result, 'memory': memory_result})

    @staticmethod
    def exposed_stop(exec_id, job_id, pid):
        """停止正在进行中的调度任务"""
        # kill -9 进程id
        if os.name == 'nt':
            # Windows系统
            cmd = 'taskkill /pid %s /f' % pid
            os.system(cmd)
        elif os.name == 'posix':
            # Linux系统
            cmd = 'kill -9 %s' % pid
            os.system(cmd)
        else:
            raise Exception('Undefined os.name')
        # 修改调度详情表, 分布式锁
        with MysqlLock(config.mysql.etl, 'exec_lock_%s' % exec_id):
            SchedulerModel.update_exec_job_status(db.etl_db, exec_id, job_id, 'failed')
        log.error('停止调度, 执行id: %s, 任务id: %s, 任务状态: %s' % (exec_id, job_id, 'failed'))
        return {'status': True, 'msg': '任务已停止'}
