# coding=utf-8
# author=veficos

from rpyc.utils.server import ThreadedServer
from werkzeug._reloader import run_with_reloader
import rpyc
import threading
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from configs import config, app, scheduler
from rpc.rpc_server import RPCServer
from scheduler.scheduler_listener import listener


def scheduler_instant():
    """调度实例"""
    scheduler.init_app(app)
    # 添加全局监听器
    scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.start()


def rpc_instant():
    """rpc服务端"""
    t3 = ThreadedServer(RPCServer, port=config.exec.port, protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t3.start()


def inner():
    """启动服务"""
    # 调度对象
    t1 = threading.Thread(target=scheduler_instant)
    t1.start()
    # rpc服务端
    rpc_instant()


if __name__ == '__main__':
    run_with_reloader(inner)
