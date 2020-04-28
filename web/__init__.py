#!/bin/python
# -*- coding: utf-8 -*-
# @File  : __init__.py.py
# @Author: wangms
# @Date  : 2020/4/28
# @Brief: 简述报表功能

from flask import Blueprint

web = Blueprint('web', __name__)

from web import view