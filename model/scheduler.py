# !/usr/bin/env python
# -*- coding: utf-8 -*-

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
    def get_exec_job(cursor, exec_id, job_id):
        """获取执行任务"""
        command = '''
        SELECT in_degree, run_period
        FROM tb_execute_detail
        LEFT JOIN tb_jobs USING(job_id)
        WHERE exec_id = :exec_id AND job_id = :job_id
        '''
        result = cursor.query_one(command, {
            'exec_id': exec_id,
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
    def exec_job_start(cursor, exec_id, job_id):
        """执行任务开始"""
        command = '''
        UPDATE tb_execute_detail
        SET status = 'running', insert_time = UNIX_TIMESTAMP(), update_time = UNIX_TIMESTAMP()
        WHERE exec_id = :exec_id AND job_id = :job_id
        '''
        result = cursor.update(command, {
            'exec_id': exec_id,
            'job_id': job_id
        })
        return result

    @staticmethod
    def update_exec_job_status(cursor, exec_id, job_id, status):
        """修改执行任务状态"""
        command = '''
        UPDATE tb_execute_detail
        SET status = :status, update_time = UNIX_TIMESTAMP()
        WHERE exec_id = :exec_id AND job_id = :job_id
        '''
        result = cursor.update(command, {
            'exec_id': exec_id,
            'job_id': job_id,
            'status': status
        })
        return result