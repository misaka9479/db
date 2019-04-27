#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/24 9:45
# @Author  : Bohan Li

NOTE = \
    [
        {'DATABASE': '行情指标',
         'COLLECTION': ['前收盘价', '开盘价', '最高价', '最低价', '收盘价'],
         'NOTE1': {'0': '不复权',
                   '1': '前复权',
                   '2': '后复权',
                   '3': '定点复权'}
         },

        {'DATABASE': '股东指标',
         'COLLECTION': ['大股东持股比例'],
         'NOTE1': {'1': '大股东排名第1名'}
         }
    ]
