#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_reader.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from .transformation import Transformation

class DataReader:
    def __init__(self, dp):
        self.dp = dp
        self.dataframe = None
        self.data_source = None

    def get(self)->Transformation:
        return Transformation(self.dp, self.dataframe)











