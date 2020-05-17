#!/bin/python
# -*- coding: utf-8 -*-
# @File  : data_sync.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from threading import Thread
from .data_writer import DataWriter
from .transformation import Transformation
from .relation_database import RelationDatabaseReader, RelationDatabaseWriter


class DataPlump:
    def __init__(self):
        self.data_reader = None
        self.data_writers = {}
        self._task = []

    @property
    def read(self):
        return self

    @property
    def write(self):
        return self

    def start(self):
        for writer, trans in self.data_writers.items():
            t = Thread(target=writer.write, args=(trans.dataframe,))
            self._task.append(t)

    def wait(self):
        for t in self._task:
            t.start()
        for t in self._task:
            t.join()

    @property
    def from_rdb(self):
        self.data_reader = RelationDatabaseReader(self)
        return self.data_reader

    @property
    def from_file(self):
        return

    @property
    def from_rest(self):
        return

    @property
    def from_ftp(self):
        return

    @property
    def to_rdb(self):
        writer = RelationDatabaseWriter(self)
        self.data_writers[writer] = self.transformation
        return writer

    @property
    def to_file(self):
        return self

    def _backfill(self, tf: Transformation):
        self.transformation = tf