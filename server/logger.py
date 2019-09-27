# coding=utf-8
# author=veficos

import logging
from logging import handlers


def create_logger(conf, name):
    """日志对象"""
    logger = logging.getLogger(name)
    level = logging.getLevelName(conf['log']['level'])
    logger.setLevel(level)
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s [%(pathname)s:%(funcName)s:%(lineno)d] %(message)s')
    # fluented异步流日志
    # handler = FluentHandler(tag=conf['tag'], host=conf['host'], port=conf['port'])
    # handler.setFormatter(FluentRecordFormatter(fmt={
    #     'level': '%(levelname)s',
    #     'sys_host': '%(hostname)s',
    #     'sys_name': '%(name)s',
    #     'sys_module': '%(module)s',
    #     'function': '[%(pathname)s:%(funcName)s:%(lineno)d]',
    #     'stack_trace': '%(exc_text)s'
    # }))
    # handler.setLevel(level)
    # logger.addHandler(handler)
    # 日志文件
    file_handler = handlers.RotatingFileHandler('logs/exec.log', maxBytes=2 ** 30, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(conf['log']['level'])
    logger.addHandler(file_handler)
    # 控制台
    if conf['env']['deploy'] == 'dev':
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(stream_handler)

    return logger
