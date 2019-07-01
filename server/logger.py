# coding=utf-8
# author=veficos

import logging
from fluent.asynchandler import FluentHandler
from fluent.handler import FluentRecordFormatter


def create_logger(conf, name):
    """日志对象"""
    # 异步流
    logger = logging.getLogger(name)
    level = logging.getLevelName(conf['level'])
    logger.setLevel(level)
    handler = FluentHandler(tag=conf['tag'], host=conf['host'], port=conf['port'])
    handler.setFormatter(FluentRecordFormatter(fmt={
        'level': '%(levelname)s',
        'sys_host': '%(hostname)s',
        'sys_name': '%(name)s',
        'sys_module': '%(module)s',
        'function': '[%(pathname)s:%(funcName)s:%(lineno)d]',
        'stack_trace': '%(exc_text)s'
    }))
    handler.setLevel(level)
    logger.addHandler(handler)
    # 控制台
    if conf['env'] == 'dev':
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter('%(asctime)s %(levelname)s [%(pathname)s:%(funcName)s:%(lineno)d] %(message)s'))
        stream_handler.setLevel(level)
        logger.addHandler(stream_handler)

    return logger
