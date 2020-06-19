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
from .greenplum_database import GreenplumDatabaseWriter


class DataPump:
    def __init__(self, logger=None):
        self.logger = logger
        self.readers = []
        self.writers = []

        if not self.logger:
            self._init_logger()

    def read(self, parallel=False):
        return DataPumpReader(self, self.logger, parallel)

    def write(self, parallel=False):
        return DataPumpWriter(self, self.logger, parallel)

    def make_dataframe(self, columns: list, values: list):
        return _DataFrame(columns, values)

    def _init_logger(self):
        import logging
        self.logger = logging.getLogger("DataPump")
        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG)
            sh = logging.StreamHandler()
            sh.setFormatter(logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s'))
            sh.setLevel(logging.DEBUG)
            self.logger.addHandler(sh)

    def wait_reader(self):
        for reader in self.readers:
            reader.wait()
        return [i.dataframe for i in self.readers]

    def wait_writer(self):
        for writer in self.writers:
            writer.wait()


class DataPumpReader:
    def __init__(self, parent, logger, enable_thread):
        self.parent = parent
        self.logger = logger
        self.enable_thread = enable_thread

    def from_rdb(self, db_type):
        reader = RelationDatabaseReader(
            db_type=db_type,
            logger=self.logger,
            enable_thread=self.enable_thread)
        self.parent.readers.append(reader)
        return reader

    def from_file(self):
        pass

    def from_rest(self):
        pass

    def from_ftp(self):
        pass


class DataPumpWriter:
    def __init__(self, parent, logger, enable_thread):
        self.parent = parent
        self.logger = logger
        self.enable_thread = enable_thread

    def with_data(self, data: _DataFrame):
        self.data = data
        return self

    def to_rdb(self, db_type):
        db_type_class_map = {
            "greenplum": GreenplumDatabaseWriter,
            "postgresql": PostgreSQLDatabaseWriter
        }
        db_type = db_type.lower()
        if db_type not in db_type_class_map:
            raise Exception(f"The writer of database <{db_type}> hasn't been implemented")
        writer = db_type_class_map[db_type](db_type=db_type, logger=self.logger, enable_thread=self.enable_thread)
        writer._set_data(self.data)
        self.parent.writers.append(writer)
        return writer

    def to_file(self):
        pass

    def to_rest(self):
        pass

    def to_ftp(self):
        pass

    def to_console(self):
        x = PrettyTable()
        x.field_names = self.data.columns
        for i in self.data:
            x.add_row(i)
        print(x)
