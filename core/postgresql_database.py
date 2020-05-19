#!/bin/python
# -*- coding: utf-8 -*-
# @File  : postgresql_database.py
# @Author: wangms
# @Date  : 2020/5/13
# @Brief: 简述报表功能
import os
import logging
from traceback import format_exc
import subprocess
from functools import reduce
from .data_frame import _DataFrame
from .utils import WriteMode
from .relation_database import RelationDatabaseWriter
from random import randint


class PostgreSQLDatabaseWriter(RelationDatabaseWriter):

    def write(self, df: _DataFrame):
        if len(self._columns) == 0:
            self._columns = df.columns

        logging.debug(f"The number of records that need to be synchronized: {df.rows}")

        if self._mode == WriteMode.upsert:
            if self.db_type.lower() == "postgresql":
                self.sql = f"INSERT INTO {self.table_name} ({','.join(self._columns)}) " \
                           f"VALUES ({','.join(['%s'] * len(self._columns))}) "

                self.sql += f"ON CONFLICT ({','.join(self._upsert_by_columns)}) "

                if self._upsert_conflict_update_columns:
                    self.sql += "DO UPDATE SET "
                    for k, v in self._upsert_conflict_update_columns:
                        self.sql += f"""{k}={'EXCLUDED.' + k if v is None else v if isinstance(v, (int, float)) else "'" + v + "'"}, """
                    else:
                        self.sql = self.sql.strip().strip(",")
                else:
                    self.sql += "DO NOTHING"

                with self.db_conn.cursor() as cursor:
                    self.db_conn.set_session(autocommit=False)
                    batch_size = 0
                    for i in df:
                        cursor.execute(self.sql, i)
                        batch_size += 1

                        if batch_size == self._batch_size:
                            self.db_conn.commit()
                            batch_size = 0
                    self.db_conn.commit()

            elif self.db_type.lower() == "greenplumn":
                with self.db_conn.cursor() as cursor:
                    temp_load_table = f"tmp_{self.table_name.split('.')[-1]}_{randint(1000, 1100)}"
                    cursor.execute(f"create temp table {temp_load_table} as "
                                   f"select {','.join(self._columns)} from {self.table_name} where 1=2")
                    fr, fw = os.pipe()

                    pid = os.fork()
                    if pid:
                        os.close(fw)
                        try:
                            cursor.copy_from(os.fdopen(fr), temp_load_table, columns=self._columns, sep=self._delimiter,
                                             null="")
                            cursor.execute(f"select count(1) from {temp_load_table}")
                            logging.debug(f"copy: {cursor.fetchone()}")

                            conflict_data_table = f"tmp_conflict_data_{randint(1101, 1200)}"
                            cursor.execute(f"create temp table {conflict_data_table} as " \
                                           f"select a.* from {temp_load_table} a, {self.table_name} b " \
                                           f"where 1=1 {' '.join(['and a.' + col + '=' + 'b.' + col for col in self._upsert_by_columns])}")
                            cursor.execute(f"select count(1) from {conflict_data_table}")
                            logging.debug(f"conflict: {cursor.fetchone()}")

                            update_segment = []
                            for k, v in self._upsert_conflict_update_columns:
                                if v is None:
                                    update_segment.append(f'{k}=a.{k}')
                                else:
                                    if isinstance(v, (int, float)):
                                        update_segment.append(f'{k}={v}')
                                    else:
                                        update_segment.append(f"{k}='{v}'")

                            cursor.execute(f"update {self.table_name} a " \
                                           f"set {','.join(update_segment)} " \
                                           f"from {conflict_data_table} b " \
                                           f"where 1=1 {' '.join(['and a.' + col + '=' + 'b.' + col for col in self._upsert_by_columns])}")

                            logging.debug(f"update: {cursor.rowcount}")

                            cursor.execute(f"insert into {self.table_name} ({','.join(self._columns)}) " \
                                           f"select a.* "
                                           f"from {temp_load_table} a "
                                           f"left join {conflict_data_table} b "
                                           f"on 1=1 {' '.join(['and a.' + col + '=' + 'b.' + col for col in self._upsert_by_columns])} " \
                                           f"where b.{self._upsert_by_columns[0]} is null")
                            logging.debug(f"insert: {cursor.rowcount}")
                            self.db_conn.commit()
                        except:
                            logging.debug(format_exc())
                            logging.debug(cursor.query)
                    else:
                        os.close(fr)
                        with os.fdopen(fw, "w") as f:
                            for index, line in enumerate(df):
                                f.write(reduce(lambda x, y: f"{x}{self._delimiter}{'' if y is None else y}",
                                               line).strip() + "\n")

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
                for index, line in enumerate(df):
                    a = reduce(lambda x, y: f"{x}{self._delimiter}{'' if y is None else y}", line)
                    f.write(a + "\n")

            out, err = x.communicate()
            logging.debug(f"out: {out}, error: {err}")
