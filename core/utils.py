#!/bin/python
# -*- coding: utf-8 -*-
# @File  : utils.py
# @Author: wangms
# @Date  : 2020/5/13
# @Brief: 简述报表功能
from io import StringIO
from enum import IntEnum

class _StringIO(StringIO):
    def read(self, *args, **kwargs):
        return self.getvalue()


class WriteMode(IntEnum):
    append = 1
    overwrite = 2
    upsert = 3
    only_insert_not_exists = 4
    only_update_exists = 5