# !/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

from configs import config, log


def request(exec_id, interface_id, job_id, status):
    """封装requests请求"""
    url = 'http://%(host)s:%(port)s/execute/callback/' % {'host': config.master.host, 'port': config.master.port}
    params = {'exec_id': exec_id, 'interface_id': interface_id, 'job_id': job_id, 'status': status}

    for _ in range(5):
        try:
            response = requests.get(url=url, params=params)
            log.debug('回调web服务响应结果：[status_code: %d][text: %s]' % (response.status_code, response.text))
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            log.error('回调web服务失败: [params: %s][error: %s]' % (str(params), e), exc_info=True)
            return False


def event_request(exec_id, interface_id, job_id, status):
    """封装requests请求"""
    url = 'http://%(host)s:%(port)s/event/callback/' % {'host': config.master.host, 'port': config.master.port}
    params = {'exec_id': exec_id, 'interface_id': interface_id, 'job_id': job_id, 'status': status}

    for _ in range(5):
        try:
            response = requests.get(url=url, params=params)
            log.debug('回调web服务响应结果：[status_code: %d][text: %s]' % (response.status_code, response.text))
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            log.error('回调web服务失败: [params: %s][error: %s]' % (str(params), e), exc_info=True)
            return False