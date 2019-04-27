#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/27 12:43
# @Author  : Bohan Li

import datetime
import pandas as pd
from pymongo import MongoClient

if __name__ == '__main__':
    client_raw = MongoClient(host='139.199.125.235', port=8888)
    client_factors = MongoClient(host='139.199.125.235', port=9999)

    date = '20190423'  # date将在实际运行时由代码生成

    # 52-week high
    f_DATE_gte = (datetime.datetime.strptime(date, '%Y%m%d').date() - datetime.timedelta(1 * 7)).strftime('%Y%m%d')  # todo
    f_DATE_lte = date
    data = client_raw['行情指标']['前收盘价'].find({'DATE': {'$gte': f_DATE_gte, '$lte': f_DATE_lte}}).limit(1000)  # todo
    data = pd.DataFrame(data)
    data['VALUE'] = data['VALUE'].astype(float)
    data = data.groupby('CODE1')['VALUE'].max()
    client_factors['FINANCIAL']['52WH'].insert_many([{'CODE1': CODE1, 'VALUE': str(VALUE), 'DATE': date} for CODE1, VALUE in data.items()])
