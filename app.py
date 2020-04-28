#!/bin/python
# -*- coding: utf-8 -*-
# @File  : app.py
# @Author: wangms
# @Date  : 2020/4/28
# @Brief: 简述报表功能
from flask import Flask

app = Flask(__name__)

from web import web

app.register_blueprint(web)




if __name__ == '__main__':
    app.run()


