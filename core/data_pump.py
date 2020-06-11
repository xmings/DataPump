#!/bin/python
# -*- coding: utf-8 -*-
# @File  : test_data_sync.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from threading import current_thread
from prettytable import PrettyTable
from .data_frame import _DataFrame
from .relation_database import RelationDatabaseReader
from .postgresql_database import PostgreSQLDatabaseWriter
from .greenplumn_database import GreenplumnDatabaseWriter


class DataPump:
    def __init__(self, logger=None):
        self.logger = logger
        self.dp_readers = []
        self.dp_writers = []

        if not self.logger:
            self._init_logger()

    @property
    def read(self):
        return DataPumpReader(self.logger)

    @property
    def write(self):
        return DataPumpWriter(self.logger)

    def _init_logger(self):
        import logging
        self.logger = logging.getLogger("DataPump")
        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG)
            sh = logging.StreamHandler()
            sh.setFormatter(logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s'))
            sh.setLevel(logging.DEBUG)
            self.logger.addHandler(sh)


class DataPumpReader:
    def __init__(self, logger):
        self.logger = logger

    def from_rdb(self, db_type):
        self.data_reader = RelationDatabaseReader(dp=self, db_type=db_type, logger=self.logger)
        return self.data_reader

    def from_file(self):
        pass

    def from_rest(self):
        pass

    def from_ftp(self):
        pass


class DataPumpWriter:
    def __init__(self, logger):
        self.logger = logger

    def with_data(self, data: _DataFrame):
        self.data = data
        return self

    def to_rdb(self, db_type):
        db_type_class_map = {
            "greenplumn": GreenplumnDatabaseWriter,
            "postgresql": PostgreSQLDatabaseWriter
        }

        if db_type not in db_type_class_map:
            raise Exception(f"The writer of database <{db_type}> hasn't been implemented")
        writer = db_type_class_map[db_type](dp=self, db_type=db_type, logger=self.logger)
        writer._set_data(self.data)
        return writer

    def to_file(self):
        pass

    def to_rest(self):
        pass

    def to_ftp(self):
        pass

    def to_console(self):
        x = PrettyTable()
        x.field_names  = self.data.columns
        for i in self.data:
            x.add_row(i)
        print(x)



