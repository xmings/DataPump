#!/bin/python
# -*- coding: utf-8 -*-
# @File  : relation_database.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
import json
from .data_reader import DataReader
from .data_writer import DataWriter
from .data_frame import _DataFrame
from traceback import format_exc
from .utils import WriteMode, SameAsExclude

class RelationDatabase:
    def __init__(self, db_type):
        self.db_type = db_type
        self.db_conn = None
        self._conn_config = None

    def connect(self, host: str, port: str, user: str, password: str, db_name: str=None):
        if self.db_type in ("postgresql", "greenplum"):
            self._conn_config = {
                "host": host,
                "port": port,
                "dbname": db_name,
                "user": user,
                "password": password
            }

            from psycopg2 import connect

            self.db_conn = connect(**self._conn_config)
            self.db_conn.set_session(autocommit=False)

        return self

    def commit(self, batch_size:int):
        self._batch_size = batch_size
        return self


class RelationDatabaseReader(RelationDatabase, DataReader):
    def __init__(self, db_type, enable_thread=False, logger=None):
        RelationDatabase.__init__(self, db_type)
        DataReader.__init__(self, enable_thread, logger)
        self.sql = None

    def from_sql(self, sql: str):
        self.sql = sql
        return self

    def from_table(self, table_name):
        self.sql = f"select * from {table_name}"
        return self

    def _read(self):
        with self.db_conn.cursor() as cursor:
            cursor.execute(self.sql)
            self.dataframe = _DataFrame(
                columns=list(map(lambda x:x.name, cursor.description)),
                values=cursor.fetchall()
            )
        self.db_conn.close()


class RelationDatabaseWriter(RelationDatabase, DataWriter):
    def __init__(self, db_type, logger=None, enable_thread=False):
        RelationDatabase.__init__(self, db_type)
        DataWriter.__init__(self, enable_thread, logger)
        self.table_name = ""
        self.sql = ""
        self._batch_size = 100
        self._columns = []
        self._upsert_by_columns = []
        self._conflict_specified_update_columns = {}
        self._conflict_update_others = False
        self._conflict_exclude_update_columns = []
        self._mode = WriteMode.append
        self._delimiter = '|'
        self._data = None

    def _set_data(self, data):
        self._data = data

    def connect(self, host: str, port: str, user: str, password: str, db_name: str=None):
        super().connect(host, port, user, password, db_name)
        return self

    def upsert_by(self, *args):
        assert self._mode == WriteMode.upsert, "error write mode"
        self._upsert_by_columns = args
        return self
    
    def write_mode(self, mode):
        assert mode in WriteMode, f"unknown write mode: {mode}"
        self._mode = mode
        return self

    def conflict_action(self, update_columns: list=(),
                        update_others_columns=False, update_exclude_columns: list=(), **kwargs):
        if not self._upsert_by_columns:
            raise Exception("Method on_conflict() must be called after method upsert_by()")

        if update_columns and update_others_columns:
            raise Exception("Either `update_columns` or `update_others_columns` parameter be specified")

        if update_columns:
            for col in update_columns:
                self._conflict_specified_update_columns[col] = SameAsExclude()
        else:
            if update_others_columns:
                self._conflict_update_others = True

        if update_exclude_columns:
            self._conflict_exclude_update_columns = update_exclude_columns

        for col, value in kwargs.items():
            self._conflict_specified_update_columns[col] = value
        return self


    def _get_conflict_udpate_column(self):
        update_columns = self._conflict_specified_update_columns.copy()
        not_update_columns = list(self._upsert_by_columns) + list(self._conflict_exclude_update_columns)

        if self._conflict_update_others:
            for col in self._columns:
                if col not in not_update_columns and col not in update_columns:
                    update_columns[col] = SameAsExclude()

        for col in not_update_columns:
            if col in update_columns:
                update_columns.pop(col)

        return update_columns

    def _simple_covert_column_value(self, v, level=0):
        if v is None:
            return "null"
        elif isinstance(v, (int, float)):
            return v
        elif isinstance(v, dict) and level == 0:
            return f"'{json.dumps(v)}'::json"
        elif isinstance(v, list) and all(map(lambda x: isinstance(x, dict), v)) and level == 0:
            return f"'{json.dumps(v)}'::json"
        elif isinstance(v, (list, tuple, set)) and level == 0:
            level += 1
            return "'{" + ','.join([self._simple_covert_column_value(i, level) for i in v]) + "}'::text[]"
        else:
            return "'" + str(v) + "'"

    def to_table(self, table_name: str, column_names: list=()):
        self.table_name = table_name
        self._columns = column_names
        return self

    def start(self):
        try:
            if self._mode == WriteMode.upsert and not self._upsert_by_columns:
                raise Exception("upsert columns Must be specified while using upsert mode")
            super().start()
        except:
            self.logger.error(f"Failed in writing data: "
                              f"<target-table: {self.table_name,}, write-mode: {self._mode.name}>. \n"
                              f"\n {format_exc()}")







