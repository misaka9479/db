#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/28 14:25
# @Author  : Bohan Li


from WindPy import w
from pymongo import MongoClient


def get(codes, fields, options, name, note1=None, note2=None):
    global date
    global data_dict
    d = w.wss(codes, fields, options)
    for c, v in zip(d.Codes, d.Data[0]):
        if note2:
            data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v), 'NOTE1': note1, 'NOTE2': note2})
        elif note1:
            data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v), 'NOTE1': note1})
        else:
            data_dict[c].append({'DATE': str(date), 'NAME': str(name), 'VALUE': str(v)})


if __name__ == '__main__':
    client = MongoClient(host='139.199.125.235', port=8888)

    # ////////// DATABASE(MACRO), COLLECTION(利率汇率), DATE, TIME, NAME, VALUE, NOTE1, NOTE2
    w.start()
    date = '20190423'  # date = datetime.date.today().strftime('%Y%m%d')

    # ////////// 国民经济核算
    # ////////// 工业
    # ////////// 价格指数
    # ////////// 对外贸易及投资
    # ////////// 固定资产投资
    # ////////// 国内贸易
    # ////////// 银行与货币

    # ////////// 利率汇率

    # /// 定期存款利率:1年(整存整取)
    d = w.edb("M0009808", "2000-01-01", date)
    for t, v in zip(d.Times, d.Data[0]):
        client['MACRO']['利率汇率'].insert_one({'DATE': t.strftime('%Y%m%d'), 'NAME': '定期存款利率:1年(整存整取)', 'VALUE': str(v)})

    # ////////// 证券市场
    # ////////// 财政
    # ////////// 就业与工资
    # ////////// 景气调查
    # ////////// 人民生活
    # ////////// 人口与资源
    # ////////// 教育与科技
    # ////////// 公共管理
    # ////////// 区县数据
