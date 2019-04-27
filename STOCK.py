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


def df2dict(df, column, note1=None, note2=None, func=None):
    df = df[['代码', '日期', column]]
    df = df.rename(columns={'代码': 'CODE1', '日期': 'DATE', column: 'VALUE'})
    df = df.dropna()
    if func:
        df['VALUE'] = df['VALUE'].apply(func)
    if note1:
        df['NOTE1'] = note1
    if note2:
        df['NOTE2'] = note2
    df = df.astype(str)
    return [df.loc[i].to_dict() for i in df.index]


def insert(df, collection_name, column, note1=None, note2=None, func=None):
    df = df2dict(df, column=column, note1=note1, note2=note2, func=func)
    if df:
        client['行情指标'][collection_name].insert_many(df)


if __name__ == '__main__':

    client = MongoClient(host='139.199.125.235', port=8888)
    '''
    # ////////// 行情指标
    data = {i: pd.read_csv(os.path.join('csv', i), encoding='cp936') for i in os.listdir('csv')}
    for i, df in data.items():
        print(i)
        # /// 前收盘价, 不复权
        insert(df, '前收盘价', '前收盘价(元)', '0', None, None)

        # /// 开盘价, 不复权
        insert(df, '开盘价', '开盘价(元)', '0', None, None)

        # /// 最高价, 不复权
        insert(df, '最高价', '最高价(元)', '0', None, None)

        # /// 最低价, 不复权
        insert(df, '最低价', '最低价(元)', '0', None, None)

        # /// 收盘价, 不复权
        insert(df, '收盘价', '收盘价(元)', '0', None, None)

        # /// 均价
        insert(df, '均价', '均价(元)', None, None, lambda x: '{:.4f}'.format(x))

        # /// 涨跌
        insert(df, '涨跌', '涨跌(元)', None, None, None)

        # /// 涨跌幅
        insert(df, '涨跌幅', '涨跌幅(%)', None, None, lambda x: '{:.4f}'.format(x))

        # /// 换手率
        insert(df, '换手率', '换手率(%)', None, None, None)

        # /// 换手率(自由流通股本)

        # /// 成交量
        insert(df, '成交量', '成交量(股)', None, None, None)

        # /// 成交额
        insert(df, '成交额', '成交金额(元)', None, None, None)

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
    '''

    # ////////// API数据, DATABASE, COLLECTION, CODE1, CODE2, DATE, TIME, VALUE, NOTE1, NOTE2
    w.start()

    # date = datetime.date.today().strftime('%Y%m%d')
    date = '20190423'  # date是8位日期字符串, 是在从API取数据时, 向API提供的日期, 也是在数据库中增加记录时的DATE字段的值
    # codes是全部A股的code, 在向API取数据时通过codes进行遍历
    codes = w.wset("sectorconstituent", "date={};sectorid=a001010100000000".format(date)).Data[1]
    codes = codes[:10]  # 写代码时不用请求全部的

    # ////////// 基本资料
    # ////////// 股本指标

    # ////////// 股东指标
    # /// 大股东持股比例
    for i in range(1, 11):  # 大股东持股比例有附加参数"大股东排名", 取值1-10, 在这里进行遍历
        rsp = w.wss(codes, "holder_pct", "tradeDate={};order={}".format(date, i))  # 向API请求数据
        data = zip(rsp.Codes, rsp.Data[0])
        client['股东指标']['大股东持股比例'].insert_many([{'CODE1': code, 'DATE': date, 'VALUE': str(value), 'NOTE1': str(i)} for code, value in data if not math.isnan(value)])  # 向数据库插入多条数据

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
