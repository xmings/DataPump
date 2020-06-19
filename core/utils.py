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
    insert_not_exists = 4
    update_exists = 5

class SameAsExclude:
    pass


import operator


class Column:
    def __init__(self, name):
        self.name = name

    def lt(self, other):
        return Condition(self.name, operator.lt, other.name)

    def gt(self, other):
        return Condition(self.name, operator.gt, other.name)

    def le(self, other):
        return Condition(self.name, operator.le, other.name)

    def ge(self, other):
        return Condition(self.name, operator.ge, other.name)

    def eq(self, other):
        return Condition(self.name, operator.eq, other.name)

    def ne(self, other):
        return Condition(self.name, operator.ne, other.name)

class Condition:
    def __init__(self, left_column, operator, right_column):
        self.left_column = left_column
        self.operator = operator
        self.right_column = right_column

    def run(self, row1, row2):
        return self.operator(getattr(row1, self.left_column), getattr(row2, self.right_column))

    def and_(self, other):
        if isinstance(other, Condition):
            other = ConditionGroup(other)

        return ConditionGroup(self).and_(other)

    def or_(self, other):
        if isinstance(other, Condition):
            other = ConditionGroup(other)

        return ConditionGroup(self).or_(other)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"Condition[left: {self.left_column}, operator: {self.operator.__name__}, right: {self.right_column}]"

class ConditionGroup(list):
    def __init__(self, condition: Condition=None):
        super().__init__()
        if condition:
            self.append(condition)

    def copy(self):
        new_condition_group = ConditionGroup()
        for condition in self:
            new_condition_group.append(condition)
        return new_condition_group

    def and_(self, other):
        if isinstance(other, Condition):
            self.append(other)
        elif isinstance(other, ConditionGroup):
            for cond in other:
                self.append(cond)
        elif isinstance(other, tuple):
            new_condition_group = []
            for o in other:
                my: ConditionGroup = self.copy()
                new_condition_group.append(my.and_(o))
            return new_condition_group
        else:
            raise Exception(f"Unknown parameter: {other}")
        return self

    def or_(self, other):
        if isinstance(other, Condition):
            other = ConditionGroup(other)

        return self, other

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "ConditionGroup{" + ", ".join([str(i) for i in self])+ "}"


