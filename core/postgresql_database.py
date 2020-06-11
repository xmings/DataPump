#!/bin/python
# -*- coding: utf-8 -*-
# @File  : postgresql_database.py
# @Author: wangms
# @Date  : 2020/5/13
# @Brief: 简述报表功能
import os
from traceback import format_exc
import subprocess
from functools import reduce
from .utils import WriteMode, SameAsExclude
from .relation_database import RelationDatabaseWriter


class PostgreSQLDatabaseWriter(RelationDatabaseWriter):
    def write(self):
        if len(self._columns) == 0:
            self._columns = self._data.columns

        if self._mode == WriteMode.upsert:
            self.sql = f"INSERT INTO {self.table_name} ({','.join(self._columns)}) " \
                       f"VALUES ({','.join(['%s'] * len(self._columns))}) "

            self.sql += f"ON CONFLICT ({','.join(self._upsert_by_columns)}) "

            update_segment = []
            for k, v in self._get_conflict_udpate_column().items():
                if isinstance(v, SameAsExclude):
                    target_column = self._data.columns[self._columns.index(k)]
                    update_segment.append(f"{k} = EXCLUDED.{target_column}")
                else:
                    update_segment.append(f"{k} = {self._simple_covert_column_value(v)}")

            if update_segment:
                self.sql += f"DO UPDATE SET {','.join(update_segment)}"
            else:
                self.sql += f"DO NOTHING "

            try:
                count = 0

                with self.db_conn.cursor() as cursor:
                    self.db_conn.set_session(autocommit=False)
                    for row in self._data:
                        cursor.execute(self.sql % tuple(self._simple_covert_column_value(i) for i in row))
                        count += 1

                        if count % self._batch_size == 0:
                            self.db_conn.commit()
                    self.db_conn.commit()
                self.logger.info(f"[{self.table_name}] total: {self._data.row_number}, upsert: {count}.")
            except:
                self.logger.error(f"sql: {cursor.query}")
                self.logger.error(format_exc())

        elif self._mode == WriteMode.overwrite or self._mode == WriteMode.append:
            if self._mode == WriteMode.overwrite:
                with self.db_conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {self.table_name}")
                    # truncate也需要手工commit
                    self.db_conn.commit()

            fr, fw = os.pipe()

            psql_command = "psql -d {dbname} -h {host} -p {port} -U {user} -c \"{cmd}\"" \
                .format(
                cmd=f"copy {self.table_name} ({reduce(lambda x, y: f'{x},{y}', self._columns)}) from stdin delimiter '|' NULL ''",
                **self._conn_config
            )

            x = subprocess.Popen(psql_command,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 stdin=fr, shell=True, encoding="utf8")

            with os.fdopen(fw, "w") as f:
                for index, line in enumerate(self._data):
                    a = reduce(lambda x, y: f"{x}{self._delimiter}{'' if y is None else y}", line)
                    f.write(a + "\n")

            out, err = x.communicate()
            if err:
                self.logger.error(f"[{self.table_name}] Failed in writing data: {err}")
            else:
                self.logger.info(f"[{self.table_name}] Successfully written {self._data.row_number} rows of data.")
