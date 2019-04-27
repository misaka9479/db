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


def df2dict(df, name_in_csv):
    df = df[['代码', '日期', name_in_csv]]
    df = df.rename(columns={'代码': 'CODE1', '日期': 'DATE', name_in_csv: 'VALUE'})
    df = df.dropna()
    df = df.astype(str)
    return [df.loc[i].to_dict() for i in df.index]


if __name__ == '__main__':

    client = MongoClient(host='139.199.125.235', port=8888)

    # ////////// 行情指标
    data = {i: pd.read_csv(os.path.join('csv', i), encoding='cp936') for i in os.listdir('csv')}
    data_dict = {}
    for i, df in data.items():
        i = i.strip('.CSV')
        data_dict[i] = {}

        # /// 前收盘价
        data_dict[i]['前收盘价'] = df2dict(df, '前收盘价(元)')

        # /// 开盘价
        data_dict[i]['开盘价'] = df2dict(df, '开盘价(元)')

        # /// 最高价
        data_dict[i]['最高价'] = df2dict(df, '最高价(元)')

        # /// 最低价
        data_dict[i]['最低价'] = df2dict(df, '最低价(元)')

        # /// 收盘价
        data_dict[i]['收盘价'] = df2dict(df, '收盘价(元)')

        # /// 均价
        data_dict[i]['均价'] = df2dict(df, '均价(元)')

        # /// 涨跌
        data_dict[i]['涨跌'] = df2dict(df, '涨跌(元)')

        # /// 涨跌幅
        data_dict[i]['涨跌幅'] = df2dict(df, '涨跌幅(%)')

        # /// 换手率
        data_dict[i]['换手率'] = df2dict(df, '换手率(%)')

        # /// 换手率(自由流通股本)

        # /// 成交量
        data_dict[i]['成交量'] = df2dict(df, '成交量(股)')

        # /// 成交额
        data_dict[i]['成交额'] = df2dict(df, '成交金额(元)')

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
        print(i)
        break

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
    for i in range(1, 2):  # 大股东持股比例有附加参数"大股东排名", 取值1-10, 在这里进行遍历
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
    '''
