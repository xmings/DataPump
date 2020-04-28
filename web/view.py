#!/bin/python
# -*- coding: utf-8 -*-
# @File  : view.py
# @Author: wangms
# @Date  : 2020/4/28
# @Brief: 简述报表功能
import os
import json
from web import web
from traceback import format_exc
from app import app
from flask import jsonify, request, Response

with app.app_context():
    job_dir = os.path.join(app.config.root_path, "jobs")

@web.route("/job/list")
def job_list():
    return jsonify(os.listdir(job_dir))


@web.route("/job")
def get_job():
    job_name = request.args.get("job_name")
    job_path = os.path.join(job_dir, job_name)
    with open(job_path, "r", encoding="utf8") as f:
        job_content = f.read()
    return jsonify({
        job_name: job_content
    })

@web.route("/job/add", methods=["POST"])
def add_job():
    job_name = request.form.get("job_name")
    job_path = os.path.join(job_dir, job_name)
    if os.path.isfile(job_path):
        return Response(response="同名作业已存在，请更换名称", status=401)
    job_content = request.form.get("job_content")
    with open(job_path, "w", encoding="utf8") as f:
        f.write(job_content)

    return Response(response="作业添加成功", status=200)


@web.route("/job/change", methods=["POST"])
def change_job():
    job_name = request.form.get("job_name")
    job_path = os.path.join(job_dir, job_name)
    job_content = request.form.get("job_content")
    with open(job_path, "w", encoding="utf8") as f:
        f.write(job_content)

    return Response(response="作业添加成功", status=200)


@web.route("/job/run", methods=["POST"])
def run_job():
    job_name = request.form.get("job_name")
    job_path = os.path.join(job_dir, job_name)
    with open(job_path, "r", encoding="utf8") as f:
        job_content = f.read()
    try:
        exec(job_content)
    except Exception:
        return Response(response=json.dumps({"status": 500, "response": "作业运行失败", "error": format_exc()}), status=500)
    return Response(response=json.dumps({"status": 200, "response": "作业运行成功"}), status=200)
