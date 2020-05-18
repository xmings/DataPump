#!/bin/python
# -*- coding: utf-8 -*-
# @File  : relation_database.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from .data_reader import DataReader
from .data_writer import DataWriter
from .data_frame import _DataFrame
from .utils import WriteMode

class RelationDatabase:
    def __init__(self):
        self.db_type = None
        self.db_conn = None
        self._conn_config = None

    def connect(self, db_type: str, config: dict):
        if db_type in ("postgresql", "greenplumn"):
            from psycopg2 import connect

            self.db_type = db_type
            self.db_conn = connect(**config)
            self.db_conn.set_session(autocommit=False)

        return self


class RelationDatabaseReader(RelationDatabase, DataReader):
    def __init__(self, dp):
        RelationDatabase.__init__(self)
        DataReader.__init__(self, dp)
        self.sql = None

    def from_sql(self, sql: str):
        self.sql = sql
        return self

    def from_table(self, table_name):
        self.sql = f"select * from {table_name}"
        return self

    def get(self):
        with self.db_conn.cursor() as cursor:
            cursor.execute(self.sql)
            self.dataframe = _DataFrame(
                columns=list(map(lambda x:x.name, cursor.description)),
                values=cursor.fetchall()
            )
            return super().get()


class RelationDatabaseWriter(RelationDatabase, DataWriter):

    def __init__(self, dp):
        RelationDatabase.__init__(self)
        DataWriter.__init__(self, dp)
        self.table_name = ""
        self.sql = ""
        self._batch_size = 100
        self._columns = []
        self._upsert_by_columns = []
        self._upsert_conflict_update_columns = []
        self._operation_time_column = ""
        self._is_truncate = False
        self._mode = WriteMode.append
        self._delimiter = '|'

    def connect(self, db_type: str, config: dict):
        super().connect(db_type, config)
        from .postgresql_database import PostgreSQLDatabaseWriter

        writer = PostgreSQLDatabaseWriter(self)
        writer.db_type = self.db_type
        writer.db_conn = self.db_conn
        wr, trans = self.dp.data_writers.popitem()
        self.dp.data_writers[writer] = trans
        return writer

    def upsert_by(self, *args):
        assert self._mode == WriteMode.upsert, "error write mode"
        self._upsert_by_columns = args
        return self
    
    def write_mode(self, mode):
        assert mode in WriteMode, f"unknown write mode: {mode}"
        self._mode = mode
        return self

    def truncate(self):
        if not self.table_name:
            raise Exception("Method truncate() must be called after method to_table()")
        self._is_truncate = True
        return self

    def conflict_action(self, update_columns: list=(), **kwargs):
        if not self._upsert_by_columns:
            raise Exception("Method on_conflict() must be called after method upsert_by()")

        for col in update_columns:
            self._upsert_conflict_update_columns.append((col, None))

        for k, v in kwargs.items():
            self._upsert_conflict_update_columns.append((k, v))
        return self

    def to_table(self, table_name: str, column_names: list=()):
        self.table_name = table_name
        self._columns = column_names
        return self

    def commit(self, batch_size:int):
        self._batch_size = batch_size
        return self

    def write(self, df: _DataFrame):
        pass







