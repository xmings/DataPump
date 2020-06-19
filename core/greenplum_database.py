#!/bin/python
# -*- coding: utf-8 -*-
# @File  : greenplum_database.py
# @Author: wangms
# @Date  : 2020/6/11
# @Brief: 简述报表功能
import os
from traceback import format_exc
import subprocess
from threading import Thread
from functools import reduce
from .utils import WriteMode, SameAsExclude
from .relation_database import RelationDatabaseWriter
from random import randint


class GreenplumDatabaseWriter(RelationDatabaseWriter):
    def _write(self):
        if len(self._columns) == 0:
            self._columns = self._data.columns

        if self._mode == WriteMode.upsert:
            with self.db_conn.cursor() as cursor:
                temp_load_table = f"tmp_{self.table_name.split('.')[-1]}_{randint(1000, 1100)}"
                cursor.execute(f"create temp table {temp_load_table} as "
                               f"select {','.join(self._columns)} from {self.table_name} where 1=2")
                fr, fw = os.pipe()
                def _copy_from_upsert():
                    try:
                        cursor.copy_from(os.fdopen(fr), temp_load_table, columns=self._columns, sep=self._delimiter,
                                         null="")

                        conflict_data_table = f"tmp_conflict_data_{randint(1101, 1200)}"
                        cursor.execute(f"create temp table {conflict_data_table} as " \
                                       f"select a.* from {temp_load_table} a, {self.table_name} b " \
                                       f"where 1=1 {' '.join(['and a.' + col + '=' + 'b.' + col for col in self._upsert_by_columns])}")

                        update_segment = []
                        for k, v in self._get_conflict_udpate_column().items():
                            if isinstance(v, SameAsExclude):
                                target_column = self._data.columns[self._columns.index(k)]
                                update_segment.append(f'{k}=b.{target_column}')
                            else:
                                update_segment.append(f"{k}={self._simple_covert_column_value(v)}")

                        update_row_number = 0
                        if update_segment:
                            cursor.execute(f"update {self.table_name} a " \
                                           f"set {','.join(update_segment)} " \
                                           f"from {conflict_data_table} b " \
                                           f"where 1=1 {' '.join(['and a.' + col + '=' + 'b.' + col for col in self._upsert_by_columns])}")
                            update_row_number = cursor.rowcount

                        cursor.execute(f"insert into {self.table_name} ({','.join(self._columns)}) " \
                                       f"select a.* "
                                       f"from {temp_load_table} a "
                                       f"left join {conflict_data_table} b "
                                       f"on 1=1 {' '.join(['and a.' + col + '=' + 'b.' + col for col in self._upsert_by_columns])} " \
                                       f"where b.{self._upsert_by_columns[0]} is null")
                        insert_row_number = cursor.rowcount
                        self.db_conn.commit()
                        self.logger.info(f"[{self.table_name}] total: {self._data.row_number}, "
                                         f"update: {update_row_number}, insert: {insert_row_number}.")
                    except:
                        self.logger.error(f"sql: {cursor.query}")
                        self.logger.error(format_exc())

                t = Thread(target=_copy_from_upsert)
                t.start()

                with os.fdopen(fw, "w") as f:
                    for index, line in enumerate(self._data):
                        f.write(reduce(lambda x, y: f"{x}{self._delimiter}{'' if y is None else y}",
                                       line).strip() + "\n")
                t.join()
                

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
