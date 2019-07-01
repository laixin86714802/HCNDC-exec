# !/usr/bin/env python
# -*- coding: utf-8 -*-

import rpyc
import datetime
from exec.run_job import start_job

from configs import log, scheduler, db, config
from model.scheduler import SchedulerModel

class RPCServer(rpyc.Service):
    """调度服务器端"""
    @staticmethod
    def exposed_execute(exec_id, job_id, server_dir, server_script, status):
        """接受任务"""
        # 添加至调度器
        run_id = 'exec_%s_%s' % (exec_id, job_id)
        kwargs = {
            'exec_id': exec_id,
            'job_id': job_id,
            'server_dir': server_dir,
            'server_script': server_script,
            'status': status
        }
        log.info('接收任务: %s' % str(kwargs))
        # 添加调度任务
        next_run_time = datetime.datetime.now() + datetime.timedelta(seconds=5)
        try:
            # 查询任务是否存在
            run_job = SchedulerModel.get_scheduler_by_id(db.etl_db, run_id, config.exec.table_name)
            if run_job:
                return {'status': False, 'msg': '该任务已在运行中'}
            # scheduler.add_job(id=run_id, func=start_job, args=(exec_id, job_id, server_dir, server_script, status), trigger='interval', seconds=3)
            scheduler.add_job(id=run_id, func=start_job, args=(exec_id, job_id, server_dir, server_script, status), next_run_time=next_run_time)
            return {'status': True, 'msg': '任务已开始运行'}
        except Exception as e:
            return {'status': False, 'msg': e}


