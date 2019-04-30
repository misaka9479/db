#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/23 9:56
# @Author  : Bohan Li


import os
import math
import datetime
import pandas as pd
from WindPy import w
from pymongo import MongoClient
import pickle

import datetime
from WindPy import w
from pymongo import MongoClient


def df2dict(df, name_in_csv, name_in_db):
    df = df[['日期', name_in_csv]]
    df = df.rename(columns={'日期': 'DATE', name_in_csv: 'VALUE'})
    df['NAME'] = name_in_db
    df = df.dropna()
    df = df.astype(str)
    return [df.loc[i].to_dict() for i in df.index]


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
    '''
    # ////////// 行情指标, DATA SOURCE=行情序列
    data = {i: pd.read_csv(os.path.join('csv', i), encoding='cp936') for i in os.listdir('csv')}
    data_dict = {}
    for i, df in data.items():
        i = i.strip('.CSV')
        data_dict[i] = []

        # /// 前收盘价
        data_dict[i] += df2dict(df, '前收盘价(元)', '前收盘价')

        # /// 开盘价
        data_dict[i] += df2dict(df, '开盘价(元)', '开盘价')

        # /// 最高价
        data_dict[i] += df2dict(df, '最高价(元)', '最高价')

        # /// 最低价
        data_dict[i] += df2dict(df, '最低价(元)', '最低价')

        # /// 收盘价
        data_dict[i] += df2dict(df, '收盘价(元)', '收盘价')

        # /// 均价
        data_dict[i] += df2dict(df, '均价(元)', '均价')

        # /// 涨跌
        data_dict[i] += df2dict(df, '涨跌(元)', '涨跌')

        # /// 涨跌幅
        data_dict[i] += df2dict(df, '涨跌幅(%)', '涨跌幅')

        # /// 换手率
        # data_dict[i] += df2dict(df, '换手率(%)', '换手率')

        # /// 成交量
        data_dict[i] += df2dict(df, '成交量(股)', '成交量')

        # /// 成交额
        data_dict[i] += df2dict(df, '成交金额(元)', '成交额')
        
        # pickle.dump(data_dict, open('data_dict.pkl', 'wb'))
        print(i)

    # data_dict = pickle.load(open('data_dict.pkl', 'rb'))
    for code, docs in data_dict.items():
        client['STOCK'][code].insert_many(docs)
    '''

    # ////////// DATABASE(STOCK), COLLECTION(000001.SZ), DATE, TIME, NAME, VALUE, NOTE1, NOTE2
    w.start()
    date = datetime.date.today().strftime('%Y%m%d')
    codes = w.wset("sectorconstituent", "date={};sectorid=a001010100000000".format(date)).Data[1]  # 全部A股codes
    codes = codes[:10]  # 写代码时不用请求全部的
    data_dict = {code: [] for code in codes}

    # ////////// 基本资料
    # ////////// 股本指标
    # ////////// 股东指标
    # ////////// 行情指标
    # ////////// 估值指标
    # /// 总市值1
    get(codes, 'ev', 'unit=1;tradeDate={}'.format(date), '总市值1')

    # ////////// 风险分析
    # ////////// 盈利预测
    # ////////// 财务分析
    # ////////// 财务报表
    # ////////// 报表附注
    # ////////// 分红指标
    # ////////// 首发指标
    # ////////// 增发指标
    # /// 增发上市日
    get(codes, 'fellow_listeddate', 'year=2014', '增发上市日')

    # /// 公开发行日
    get(codes, 'fellow_issuedate', 'year=2014', '公开发行日')

    # ////////// 配股指标
    # ////////// 可转债发行
    # ////////// 股权分置改革
    # ////////// 技术形态
    # ////////// 其他指标

    for code, docs in data_dict.items():
        client['STOCK'][code].insert_many(docs)
