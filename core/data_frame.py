#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_frame.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from .utils import Column, Condition, ConditionGroup
from operator import attrgetter
from collections import namedtuple


class _DataFrame:
    def __init__(self, columns: list, values: list):
        if not isinstance(columns, (list,tuple)) or not isinstance(values, (list,tuple)):
            raise Exception("The Type of parameter `columns|values` must be iterable object")

        if values and len(columns) != len(values[0]):
            raise Exception("The number of columns of `values` is not same as `columns`")

        self._columns = list(columns)
        Row = namedtuple("Row", self._columns)
        self._row_number = len(values)
        self._values = [Row(*i) for i in values]

    @property
    def columns(self):
        return self._columns

    @property
    def column(self):
        ColumnTuple = namedtuple("Column", self.columns)
        return ColumnTuple(*[Column(col) for col in self.columns])

    @property
    def row_number(self):
        return self._row_number

    def map(self, func, columns=()):
        values = list(map(func, self._values))
        return _DataFrame(columns, values)

    def filter(self, func):
        values = list(filter(func, self._values))
        return _DataFrame(self.columns, values)

    def join(self, others, join_type, condition_groups):
        if isinstance(condition_groups, Condition):
            condition_groups = (ConditionGroup(condition_groups),)
        elif isinstance(condition_groups, list):
            condition_groups = (condition_groups,)

        new_column = self.columns + list(map(lambda x: f"{x}_r" if x in self.columns else x, others.columns))
        NewRow = namedtuple("Row", new_column)
        new_values = []
        for row1 in self._values:
            for row2 in others._values:
                for group in condition_groups:
                    if all(cond.run(row1, row2) for cond in group):
                        new_values.append(NewRow(*(row1 + row2)))
                        break

                if join_type == "left":
                    new_values.append(NewRow(*(row1 + [None] * len(row2))))

        return _DataFrame(new_column, new_values)

    def select(self, *columns_name):
        columns_index = list(map(lambda x: self.columns.index(x), columns_name))
        values = []
        for row in self._values:
            _row = []
            for index in columns_index:
                _row.append(row[index])
            values.append(_row)
        return _DataFrame(columns_name, values)

    def with_column(self, column_name, func):
        columns = list(self._columns).copy()
        columns.append(column_name)
        values = []
        for row in self._values:
            _row = list(row).copy()
            _row.append(func(row))
            values.append(_row)

        return _DataFrame(columns, values)

    def sort(self, **columns):
        # TODO: 中文排序功能待实现
        v = list(self._values)
        for col, direction in reversed(list(columns.items())):
            v.sort(key=attrgetter(col),
                   reverse=False if direction.lower() == "asc" else True)
        return _DataFrame(self.columns, v)

    def __getitem__(self, item):
        return self._values[item]
