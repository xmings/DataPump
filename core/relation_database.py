#!/bin/python
# -*- coding: utf-8 -*-
# @File  : relation_database.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
from .data_reader import DataReader
from .data_writer import DataWriter
from .data_frame import _DataFrame

class RelationDatabase:
    def __init__(self):
        self.db_type = None
        self.db_conn = None

    def connect(self, db_type: str, config: dict):
        self.db_type = db_type
        if db_type == "postgresql":
            from psycopg2 import connect
            self.db_conn = connect(**config)

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
        self._upsert_columns = []
        self._other_columns = []

    def upsert_by(self, *args):
        self._upsert_columns = args
        self._other_columns = list(filter(lambda x: x not in self._upsert_columns, self._columns))
        return self

    def to_table(self, table_name: str, column_names: list):
        self.table_name = table_name
        assert column_names
        self._columns = column_names
        self.sql = f"""
            insert into {table_name} ({",".join(column_names)})  
            values ({",".join(["%s"]*len(column_names))})
        """
        return self

    def commit(self, batch_size:int):
        self._batch_size = batch_size
        return self

    def write(self, df: _DataFrame):
        assert len(self._upsert_columns) + len(self._other_columns) == len(df.columns)
        if self.db_type == "postgresql" and self._upsert_columns:
            self.sql += f"""
                on conflict ({",".join(self._upsert_columns)})
                do update set { ",".join([ i + "=EXCLUDED." + i for i in self._other_columns])}
            """

        with self.db_conn.cursor() as cursor:
            batch_size = 0
            for i in df:

                cursor.execute(self.sql, i)
                batch_size += 1

                if batch_size == self._batch_size:
                    self.db_conn.commit()
                    batch_size = 0
            self.db_conn.commit()







