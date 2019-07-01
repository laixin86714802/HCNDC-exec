# !/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import datetime

def utc_to_local(utc_st):
    """utc转本地时间"""
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local_st = utc_st + offset
    return local_st

def local_to_utc():
    """本地转utc时间"""
    utc_time = datetime.datetime.utcfromtimestamp(time.time())
    return utc_time
