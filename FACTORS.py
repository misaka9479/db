#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2019/4/27 12:43
# @Author  : Bohan Li

import datetime
import pandas as pd
from pymongo import MongoClient

if __name__ == '__main__':
    client_db = MongoClient(host='139.199.125.235', port=8888)
    client_factors = MongoClient(host='139.199.125.235', port=9999)

    date = '20190422'
    codes_stock = client_db['STOCK'].list_collection_names()

    '''
    52-week high
    今天的价格除以52周内的最高价
    '''
    f_DATE_gte = (datetime.datetime.strptime(date, '%Y%m%d').date() - datetime.timedelta(52 * 7)).strftime('%Y%m%d')
    f_DATE_lte = date
    data = []
    for code in codes_stock:
        foo = list(client_db['STOCK'][code].find({'DATE': {'$gte': f_DATE_gte, '$lte': f_DATE_lte}, 'NAME': '收盘价'}))
        for i in foo:
            i['CODE'] = code
            data.append(i)
    data = pd.DataFrame(data)
    data['VALUE'] = data['VALUE'].astype(float)
    data_max = data.groupby('CODE')['VALUE'].max()
    data_today = data[data['DATE'] == date].set_index('CODE')['VALUE']
    factor = data_today / data_max
    client_factors['FINANCIAL']['52WH'].insert_many([{'CODE': CODE, 'VALUE': str(VALUE), 'DATE': date} for CODE, VALUE in factor.items()])
