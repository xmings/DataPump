#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_writer.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能

class DataWriter:
    def __init__(self, dp, logger=None):
        self.dp = dp
        self.data_target = None
        self.logger = logger

    def start(self):
        self.write()

    def write(self):
        pass