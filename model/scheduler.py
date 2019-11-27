# !/usr/bin/env python
# -*- coding: utf-8 -*-

import time


class SchedulerModel(object):
    @staticmethod
    def get_scheduler_by_id(cursor, run_id, table_name):
        """获取持久化表中数据"""
        command = '''
        SELECT id, next_run_time, job_state
        FROM %s
        WHERE id = :run_id
        '''

        command = command % table_name
        result = cursor.query_one(command, {
            'run_id': run_id
        })
        return result

    @staticmethod
    def get_exec_job(cursor, exec_id, interface_id, job_id):
        """获取执行任务"""
        command = '''
        SELECT in_degree, run_period
        FROM tb_execute_detail
        LEFT JOIN tb_jobs USING(job_id)
        WHERE exec_id = :exec_id AND interface_id = :interface_id AND job_id = :job_id
        '''
        result = cursor.query_one(command, {
            'exec_id': exec_id,
            'interface_id': interface_id,
            'job_id': job_id
        })
        return result if result else {}

    @staticmethod
    def get_exec_prep_job(cursor, job_id):
        """获取执行依赖任务"""
        command = '''
        SELECT run_period
        FROM tb_jobs
        WHERE job_id = :job_id AND is_deleted = 0
        '''
        result = cursor.query_one(command, {
            'job_id': job_id
        })
        return result if result else {}

    @staticmethod
    def exec_job_start(cursor, exec_id, interface_id, job_id, pid):
        """执行任务开始"""
        command = '''
        UPDATE tb_execute_detail
        SET status = 'running', insert_time = :insert_time, update_time = :update_time, pid = :pid
        WHERE exec_id = :exec_id AND interface_id = :interface_id AND job_id = :job_id
        '''
        result = cursor.update(command, {
            'exec_id': exec_id,
            'interface_id': interface_id,
            'job_id': job_id,
            'pid': pid,
            'insert_time': int(time.time()),
            'update_time': int(time.time())
        })
        return result

    @staticmethod
    def add_exec_detail_job(cursor, exec_id, interface_id, job_id, level, server_dir, server_script, message, type):
        """添加执行任务详情日志"""
        command = '''
        INSERT INTO tb_schedule_detail_logs(exec_id, interface_id, job_id, `level`,
        server_dir, server_script, `message`, `type`, insert_time)
        VALUES (:exec_id, :interface_id, :job_id, :level, :server_dir, :server_script, :message, :type, :insert_time)
        '''
        result = cursor.insert(command, {
            'exec_id': exec_id,
            'interface_id': interface_id,
            'job_id': job_id,
            'level': level,
            'server_dir': server_dir,
            'server_script': server_script,
            'message': message,
            'type': type,
            'insert_time': int(time.time())
        })
        return result
