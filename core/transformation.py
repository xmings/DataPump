#!/bin/python
# -*- coding: utf-8 -*-
# @File  : transformation.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能

from .data_frame import _DataFrame

class Transformation:
    def __init__(self, dp, dataframe):
        self._dp = dp
        self.dataframe = dataframe
        self.source = None
        self.target = None

    def map(self, func):
        values = list(map(func, self.dataframe._values))
        return Transformation(self._dp, _DataFrame(self.dataframe.columns, values))

    def with_column(self, column_name, column_expr):
        return self

    def select(self, *args):
        return self

    def join(self, other_reader, type="inner"):
        return self

    def group_by(self, *column_name):
        return self

    def filter_by(self, func=None, column_expr:str=None):
        if func:
            values = list(filter(func, self.dataframe._values))
            return Transformation(self._dp, _DataFrame(self.dataframe.columns, values))
        elif column_expr:
            if column_expr.find(">=")>0:
                column, value = column_expr.split(">=")
                #self.data = list(filter())
            elif column_expr.find("<=")>0:
                column, value = column_expr.split("<=")
            elif column_expr.find("!=")>0:
                column, value = column_expr.split("!=")
            elif column_expr.find("<>")>0:
                column, value = column_expr.split("<>")
            elif column_expr.find(">")>0:
                column, value = column_expr.split(">")
            elif column_expr.find("<")>0:
                column, value = column_expr.split("<")
            elif column_expr.find("=")>0:
                column, value = column_expr.split("=")
            else:
                raise Exception(f"未知条件: {column_expr}")
        else:
            raise Exception("必须指定一个参数条件")
        return self

    @property
    def write(self):
        self._dp._backfill(self)
        return self._dp


