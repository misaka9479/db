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


def df2dict(df, name_in_csv, name_in_db):
    df = df[['日期', name_in_csv]]
    df = df.rename(columns={'日期': 'DATE', name_in_csv: 'VALUE'})
    df['NAME'] = name_in_db
    df = df.dropna()
    df = df.astype(str)
    return [df.loc[i].to_dict() for i in df.index]


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

        # /// 换手率(自由流通股本)
        # /// 成交量
        data_dict[i] += df2dict(df, '成交量(股)', '成交量')

        # /// 成交额
        data_dict[i] += df2dict(df, '成交金额(元)', '成交额')

        # /// 成交笔数
        # /// 振幅
        # /// 相对发行价涨跌
        # /// 相对发行价涨跌幅
        # /// 交易状态
        # /// 连续停牌天数
        # /// 停牌原因
        # /// 最近交易日期
        # /// 市场最近交易日
        # /// 复权因子
        # /// 收盘价(支持定点复权)
        # /// 涨跌停状态
        # /// AH股溢价率
        # pickle.dump(data_dict, open('data_dict.pkl', 'wb'))
        print(i)

    # data_dict = pickle.load(open('data_dict.pkl', 'rb'))
    for code, docs in data_dict.items():
        client['STOCK'][code].insert_many(docs)
    '''

    # ////////// DATABASE(STOCK), COLLECTION(000001.SZ), DATE, TIME, NAME, VALUE, NOTE1, NOTE2
    w.start()
    date = '20190423'  # date = datetime.date.today().strftime('%Y%m%d')
    codes = w.wset("sectorconstituent", "date={};sectorid=a001010100000000".format(date)).Data[1]  # 全部A股codes
    codes = codes[:10]  # 写代码时不用请求全部的

    # ////////// 基本资料
    # ////////// 股本指标
    # ////////// 股东指标
    # ////////// 行情指标
    # ////////// 估值指标
    # ////////// 风险分析
    # ////////// 盈利预测
    # ////////// 财务分析
    # ////////// 财务报表
    # ////////// 报表附注
    # ////////// 分红指标
    # ////////// 首发指标
    # ////////// 配股指标
    # ////////// 可转债发行
    # ////////// 股权分置改革
    # ////////// 技术形态
    # ////////// 其他指标
