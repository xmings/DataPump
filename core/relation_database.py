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
        if db_type in ("postgresql", "greenplumn"):
            from psycopg2 import connect
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


    def upsert_by(self, *args):
        self._upsert_by_columns = args
        return self

    def truncate(self):
        if not self.table_name:
            raise Exception("Method truncate() must be called after method to_table()")
        self._is_truncate = True

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
        if len(self._columns) == 0:
            self._columns = df.columns

        self.sql = f"INSERT INTO {self.table_name} ({','.join(self._columns)}) "\
                   f"VALUES ({','.join(['%s'] * len(self._columns))}) "
        print(df.rows)

        if self._upsert_by_columns:
            if self.db_type.lower() == "postgresql":
                self.sql += f"ON CONFLICT ({','.join(self._upsert_by_columns)}) "

                if self._upsert_conflict_update_columns:
                    self.sql += "DO UPDATE SET "
                    for k, v in self._upsert_conflict_update_columns:
                        self.sql += f"""{k}={'EXCLUDED.' + k if v is None else v if isinstance(v, (int, float)) else "'"+ v + "'"}, """
                    else:
                        self.sql = self.sql.strip().strip(",")
                else:
                    self.sql += "DO NOTHING"
            elif self.db_type.lower() == "greenplumn":
                delete_sql = f"DELETE FROM {self.table_name} WHERE 1=1 "
                for col in self._upsert_by_columns:
                    delete_sql += f"AND {col}=%s"

                with self.db_conn.cursor() as cursor:
                    for index, row in enumerate(df):
                        # print(cursor.mogrify(delete_sql, list(map(lambda x: row[self._columns.index(x)], self._upsert_by_columns))).decode("utf8"))
                        cursor.execute(delete_sql, list(map(lambda x: row[self._columns.index(x)], self._upsert_by_columns)))
                        if index % 20 == 0 and index>0:
                            self.db_conn.commit()
                            return
                    self.db_conn.commit()
        else:
            if self._is_truncate:
                with self.db_conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {self.table_name}")


        with self.db_conn.cursor() as cursor:
            self.db_conn.set_session(autocommit=False)
            batch_size = 0
            for i in df:
                # print(cursor.mogrify(self.sql, i).decode("utf8"))
                cursor.execute(self.sql, i)
                batch_size += 1

                if batch_size == self._batch_size:
                    self.db_conn.commit()
                    batch_size = 0
            self.db_conn.commit()







