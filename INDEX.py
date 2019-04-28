#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/27 17:38
# @Author  : Bohan Li


from WindPy import w
from pymongo import MongoClient

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
    data = w.wss(codes, "close", "tradeDate={};priceAdj=U;cycle=D".format(date))
    for code, value in zip(data.Codes, data.Data[0]):
        data_dict[code].append({'DATE': date, 'NAME': '收盘价', 'VALUE': value})

    # ////////// 股本指标
    # ////////// 估值指标
    # ////////// 分析指标
    # ////////// 盈利预测
    # ////////// 财务分析
    # ////////// 财务报表

    for code, docs in data_dict.items():
        client['INDEX'][code].insert_many(docs)
