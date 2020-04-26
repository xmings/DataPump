#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_writer.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能

class DataWriter:
    def __init__(self, dp):
        self.dp = dp
        self.data_target = None

    def start(self):
        return self.dp.start()