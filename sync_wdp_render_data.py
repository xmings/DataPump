#!/bin/python
# -*- coding: utf-8 -*-
# @File  : sync_wdp_render_data.py
# @Author: wangms
# @Date  : 2020/4/24
# @Brief: 简述报表功能
import yaml
import json
from core.data_plump import DataPlump
from core.transformation import Transformation


def main():
    with open("config.yml", "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
        db_config = config.get("connection").get("database")

    dp = DataPlump()

    data1: Transformation = dp.read.from_rdb \
        .connect(db_type="postgresql", config=db_config.get("dw-dev")) \
        .from_sql("""
            select data_time,river_id,indicator_code,render_data,insert_time,is_deleted
            from ds_wdp.wdp_rpt_river_render_data_list
        """) \
        .get()

    data2: Transformation = data1 \
        .map(lambda x: (x[0], x[1], x[2], json.dumps(x[3]), x[4], x[5]))
#        .filter_by(lambda x: x[1] == '68b48d2ff66042a8a19e483cba42cf73') \

    data2.write.to_rdb \
        .connect(db_type="postgresql", config=db_config.get("zooguard-dev")) \
        .to_table("wdp.rpt_river_render_data_list",
                  ["data_time", "river_id", "indicator_code", "render_data", "insert_time", "is_deleted"]) \
        .upsert_by("data_time", "river_id", "indicator_code") \
        .write_operation_time("update_time") \
        .commit(100) \
        .start()

    dp.wait()


if __name__ == '__main__':
    main()
