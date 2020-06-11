#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_reader.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from .data_frame import _DataFrame

class DataReader:
    def __init__(self, dp, logger=None):
        self.dp = dp
        self.dataframe = None
        self.data_source = None
        self.logger = logger

    def get(self)->_DataFrame:
        return self.dataframe











