#!/bin/python
# -*- coding: utf-8 -*-
# @File  : utils.py
# @Author: wangms
# @Date  : 2020/5/13
# @Brief: 简述报表功能
from io import StringIO

class _StringIO(StringIO):
    def read(self, *args, **kwargs):
        return self.getvalue()