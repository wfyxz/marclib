# coding: utf-8
# __author__: u"John"
"""
This module contains global decorators
"""
import time


def time_elapse(function):
    """
    统计程序执行总时间消耗, 支持多进程的函数
    :param function:
    :return:
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = function(*args, **kwargs)
        print "function '{0}' elapse {1} seconds".format(function.__name__, time.time() - start)
        return ret
    return wrapper


def cpu_elapse(function):
    """
    统计程序执行单个cpu core的时钟消耗, 不支持多进程函数
    :param function:
    :return:
    """
    def wrapper(*args, **kwargs):
        start = time.clock()
        ret = function(*args, **kwargs)
        print "function '{0}' elapse {1} seconds".format(function.__name__, time.clock() - start)
        return ret
    return wrapper
