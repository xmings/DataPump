#!/bin/python
# -*- coding: utf-8 -*-
# @File  : postgresql_database.py
# @Author: wangms
# @Date  : 2020/5/13
# @Brief: 简述报表功能
import os
import subprocess
from functools import reduce
from .data_frame import _DataFrame
from .utils import WriteMode
from .relation_database import RelationDatabaseWriter

class PostgreSQLDatabaseWriter(RelationDatabaseWriter):


    def write(self, df: _DataFrame):
        if len(self._columns) == 0:
            self._columns = df.columns

        self.sql = f"INSERT INTO {self.table_name} ({','.join(self._columns)}) "\
                   f"VALUES ({','.join(['%s'] * len(self._columns))}) "
        print(f"The number of records that need to be synchronized: {df.rows}")

        if self._mode == WriteMode.upsert:
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
                    self.db_conn.commit()

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

        elif self._mode == WriteMode.overwrite or self._mode == WriteMode.append:
            if self._mode == WriteMode.overwrite:
                with self.db_conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {self.table_name}")

            fr, fw = os.pipe()

            psql_command = "psql -d {dbname} -h {host} -p {port} -U {user} -c \"{cmd}\""\
                .format(**self._conn_config, cmd=f"copy {self.table_name} from stdin")

            subprocess.Popen(psql_command, stdin=fr, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

            with os.fdopen(fw, "w") as f:
                for line in df:
                    f.write(reduce(lambda x, y: f"{x}{self._delimiter}{y}", line))


