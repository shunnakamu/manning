#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import datetime
import traceback


def print_log(log_str):
    d = datetime.datetime.today()
    try:
        print(d.strftime("[%Y-%m-%d %H:%M:%S] -- "), log_str)
    except UnicodeEncodeError:
        try:
            print(d.strftime("[%Y-%m-%d %H:%M:%S] -- "), log_str.encode('utf-8'))
        except UnicodeEncodeError:
            print(traceback.format_exc())
            print("unsupported str")
