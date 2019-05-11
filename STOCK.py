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
    # /// EBIT
    get(codes, 'ebit', 'unit=1;rptDate={}'.format(date), 'EBIT')

    # ////////// 财务报表
    # /// 流动资产
    get(codes, 'tot_cur_assets', 'unit=1;rptDate={};rptType=1'.format(date), '流动资产', '合并报表')

    # /// 固定资产
    get(codes, 'fix_assets', 'unit=1;rptDate={};rptType=1'.format(date), '固定资产', '合并报表')

    # /// 投资性房地产
    get(codes, 'invest_real_estate', 'unit=1;rptDate={};rptType=1'.format(date), '投资性房地产', '合并报表')

    # /// 持有至到期投资
    get(codes, 'held_to_mty_invest', 'unit=1;rptDate={};rptType=1'.format(date), '持有至到期投资', '合并报表')

    # /// 可供出售金融资产
    get(codes, 'fin_assets_avail_for_sale', 'unit=1;rptDate={};rptType=1'.format(date), '可供出售金融资产', '合并报表')

    # /// 存货
    get(codes, 'inventories', 'unit=1;rptDate={};rptType=1'.format(date), '存货', '合并报表')

    # /// 长期股权投资
    get(codes, 'long_term_eqy_invest', 'unit=1;rptDate={};rptType=1'.format(date), '长期股权投资', '合并报表')

    # /// 长期应收款
    get(codes, 'long_term_rec', 'unit=1;rptDate={};rptType=1'.format(date), '长期应收款', '合并报表')

    # /// 资产总计
    get(codes, 'tot_assets', 'unit=1;rptDate={};rptType=1'.format(date), '资产总计', '合并报表')

    # /// 流动负债
    get(codes, 'tot_cur_liab', 'unit=1;rptDate={};rptType=1'.format(date), '流动负债', '合并报表')

    # /// 短期借款
    get(codes, 'st_borrow', 'unit=1;rptDate={};rptType=1'.format(date), '短期借款', '合并报表')

    # /// 长期借款
    get(codes, 'lt_borrow', 'unit=1;rptDate={};rptType=1'.format(date), '长期借款', '合并报表')

    # /// 应付账款
    get(codes, 'acct_payable', 'unit=1;rptDate={};rptType=1'.format(date), '应付账款', '合并报表')

    # /// 所有者权益合计
    get(codes, 'tot_equity', 'unit=1;rptDate={};rptType=1'.format(date), '所有者权益合计', '合并报表')

    # /// 净利润
    get(codes, 'net_profit_is', 'unit=1;rptDate={};rptType=1'.format(date), '净利润', '合并报表')

    # /// 营业总收入
    get(codes, 'tot_oper_rev', 'unit=1;rptDate={};rptType=1'.format(date), '营业总收入', '合并报表')

    # /// 营业收入
    get(codes, 'oper_rev', 'unit=1;rptDate={};rptType=1'.format(date), '营业收入', '合并报表')

    # /// 营业总成本
    get(codes, 'tot_oper_cost', 'unit=1;rptDate={};rptType=1'.format(date), '营业总成本', '合并报表')

    # /// 营业成本
    get(codes, 'oper_cost', 'unit=1;rptDate={};rptType=1'.format(date), '营业成本', '合并报表')

    # /// 递延所得税
    get(codes, 'stmnote_incometax_5', 'unit=1;rptDate={};rptType=1'.format(date), '递延所得税', '合并报表')

    # /// 构建固定资产、无形资产和其他长期资产支付的现金
    get(codes, 'cash_pay_acq_const_fiolta', 'unit=1;rptDate={};rptType=1'.format(date), '构建固定资产、无形资产和其他长期资产支付的现金', '合并报表')

    # ////////// 报表附注
    # /// 持有至到期投资减值损失
    get(codes, 'stmnote_ImpairmentLoss_9', 'unit=1;rptDate={};rptType=1'.format(date), '持有至到期投资减值损失', '合并报表')

    # /// 固定资产-累计折旧
    get(codes, 'stmnote_assetdetail_2', 'unit=1;rptDate={}'.format(date), '固定资产-累计折旧')

    # /// 投资性房地产-累计折旧
    get(codes, 'stmnote_assetdetail_6', 'unit=1;rptDate={}'.format(date), '投资性房地产-累计折旧')

    # /// 生产性生物资产-累计折旧
    get(codes, 'stmnote_assetdetail_10', 'unit=1;rptDate={}'.format(date), '生产性生物资产-累计折旧')

    # /// 油气资源-累计折耗
    get(codes, 'stmnote_assetdetail_14', 'unit=1;rptDate={}'.format(date), '油气资源-累计折耗')

    # /// 长期投资减值准备合计
    get(codes, 'stmnote_reserve_18', 'unit=1;rptDate={};rptType=1'.format(date), '持有至到期投资减值损失', '合并报表', '期末数')

    # ////////// 分红指标
    # ////////// 首发指标
    # ////////// 增发指标
    # /// 增发上市日
    get(codes, 'fellow_listeddate', 'year=2018', '增发上市日', flag=False)

    # /// 公开发行日
    get(codes, 'fellow_issuedate', 'year=2018', '公开发行日', flag=False)

    # ////////// 配股指标
    # ////////// 可转债发行
    # ////////// 股权分置改革
    # ////////// 技术形态
    # ////////// 其他指标

    for code, docs in data_dict.items():
        client['STOCK'][code].insert_many(docs)
