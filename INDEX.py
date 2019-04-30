#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/27 17:38
# @Author  : Bohan Li


import datetime
from WindPy import w
from pymongo import MongoClient


def get(codes, fields, options, name, note1=None, note2=None, flag=True):
    global date
    global data_dict
    d = w.wss(codes, fields, options)
    for c, v in zip(d.Codes, d.Data[0]):
        # 对于特殊返回类型的特殊处理
        if isinstance(v, datetime.datetime):
            v = v.date().strftime('%Y%m%d')
        if note2:
            data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v), 'NOTE1': note1, 'NOTE2': note2})
        elif note1:
            data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v), 'NOTE1': note1})
        elif flag:
            data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v)})
        else:
            data_dict[c].append({'NAME': str(name), 'VALUE': str(v)})


if __name__ == '__main__':
    client = MongoClient(host='139.199.125.235', port=8888)

    # ////////// DATABASE(INDEX), COLLECTION(000001.SZ), DATE, TIME, NAME, VALUE, NOTE1, NOTE2
    w.start()
    date = '20190423'  # date = datetime.date.today().strftime('%Y%m%d')
    codes = ['892400.MI', '302400.MI']
    data_dict = {code: [] for code in codes}

    # ////////// 基本资料
    # ////////// 行情指标
    # /// 收盘价
    get(codes, 'close', 'tradeDate={};priceAdj=F;cycle=D'.format(date), '收盘价')

    # ////////// 股本指标
    # ////////// 估值指标
    # ////////// 分析指标
    # ////////// 盈利预测
    # ////////// 财务分析
    # ////////// 财务报表

    for code, docs in data_dict.items():
        client['INDEX'][code].insert_many(docs)
