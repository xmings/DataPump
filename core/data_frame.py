#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_frame.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能

class _DataFrame:
    def __init__(self, columns, values):
        self._columns = columns
        self._values = values

    @property
    def columns(self):
        return self._columns

    @property
    def rows(self):
        return len(self._values)

    def __getitem__(self, item):
        return self._values[item]
