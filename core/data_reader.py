#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_reader.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from .data_frame import _DataFrame
from threading import Thread

class DataReader:
    def __init__(self, enable_thread, logger=None):
        self.dataframe = None
        self.data_source = None
        self.logger = logger
        self.enable_thread= enable_thread
        self._thread = None

    def get(self)->_DataFrame:
        if self.enable_thread:
            self.thread = Thread(target=self._read)
            self.thread.daemon = True
            self.thread.start()
            return self
        else:
            self._read()
        return self.dataframe

    def _read(self):
        pass

    def wait(self):
        if self.enable_thread and self.thread.is_alive():
            self.thread.join()













