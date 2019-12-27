# !/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from configs import log
from util.request_package import request, event_request


def listener(event):
    """监听器"""
    # 获取执行id, 任务id
    [(exec_type, exec_id, interface_id, job_id)] = re.findall('(\\w+)_(\\d+)_(\\d+)_(\\d+)', event.job_id)
    # 异常
    if event.exception:
        log.warn('执行完毕, 执行id: %s, 任务流id: %s, 任务id: %s, 任务状态: %s' % (exec_id, interface_id, job_id, 'failed'))
        # 回调web服务
        if exec_type == 'exec':
            result = request(exec_id, interface_id, job_id, 'failed')
        else:
            result = event_request(exec_id, interface_id, job_id, 'failed')
        if not result:
            log.error('回调web服务失败, 执行id: %s, 任务流id: %s, 任务id: %s, 任务状态: %s' % (exec_id, interface_id, job_id, 'failed'))
    # 正常
    else:
        log.info('执行完毕, 执行id: %s, 任务流id: %s, 任务id: %s, 任务状态: %s' % (exec_id, interface_id, job_id, 'succeeded'))
        # 回调web服务
        if exec_type == 'exec':
            result = request(exec_id, interface_id, job_id, 'succeeded')
        else:
            result = event_request(exec_id, interface_id, job_id, 'succeeded')
        if not result:
            log.error(
                '回调web服务失败, 执行id: %s, 任务流id: %s, 任务id: %s, 任务状态: %s' % (exec_id, interface_id, job_id, 'succeeded'))
