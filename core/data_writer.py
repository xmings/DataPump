#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_writer.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from threading import Thread

class DataWriter:
    def __init__(self, enable_thread, logger=None):
        self.data_target = None
        self.logger = logger
        self.enable_thread = enable_thread

    def start(self):
        if self.enable_thread:
            self.thread = Thread(target=self._write)
            self.thread.daemon = True
            self.thread.start()
        else:
            self._write()

    def _write(self):
        pass

    def wait(self):
        if self.enable_thread and self.thread.is_alive():
            self.thread.join()