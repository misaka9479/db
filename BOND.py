#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/28 10:02
# @Author  : Bohan Li


from WindPy import w
from pymongo import MongoClient


def get(codes, fields, options, name):
    global date
    global data_dict
    d = w.wss(codes, fields, options)
    for c, v in zip(d.Codes, d.Data[0]):
        data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v)})


if __name__ == '__main__':
    client = MongoClient(host='139.199.125.235', port=8888)

    # ////////// DATABASE(BOND), COLLECTION(122015.SH), DATE, TIME, NAME, VALUE, NOTE1, NOTE2
    w.start()
    date = '20190423'  # date = datetime.date.today().strftime('%Y%m%d')
    codes = ['120201.SH', '120303.SH', '120486.SH', '120490.SH', '120506.SH']
    data_dict = {code: [] for code in codes}

    # ////////// 基本资料
    # ////////// 发行兑付资料
    # ////////// 行情指标
    # /// 收盘价
    get(codes, 'close', 'tradeDate={};priceAdj=F;cycle=D'.format(date), '收盘价')

    # ////////// 债券估值
    # ////////// 分析指标
    # /// 剩余期限(天)
    get(codes, 'day', 'tradeDate={}'.format(date), '剩余期限(天)')

    # ////////// 信用分析指标
    # /// 发行时债项评级
    data = w.wss(codes, "creditrating")
    for code, value in zip(data.Codes, data.Data[0]):
        data_dict[code].append({'NAME': '发行时债项评级', 'VALUE': value})

    # ////////// 机构禁投
    # ////////// 持有人指标
    # ////////// 标准券折算比例
    # ////////// 财务分析
    # ////////// 财务报表(新准则)
    # ////////// 报表附注
    # ////////// 财务报表(旧准则)
    # ////////// 利率(回购)基本资料
    # ////////// 外汇指标
    # ////////// 国债期货指标
    # ////////// ABS
    # ////////// CRM

    for code, docs in data_dict.items():
        client['BOND'][code].insert_many(docs)
