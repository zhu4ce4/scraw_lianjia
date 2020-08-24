# -*- coding: utf-8 -*-
'''
@datetime: 2020/8/23 19:56
@author: Jack Luo
@job:
//todo:
'''
import time
from concurrent.futures import thread
from concurrent.futures.thread import ThreadPoolExecutor

import threading

import requests
from scrapy.selector import Selector
import pandas as pd
import numpy as np
import re
from multiprocessing import Pool

from fake_useragent import UserAgent
from sqlalchemy import create_engine
from threading import Thread
engine = create_engine("mysql+pymysql://root:123456@localhost:3306/fang", encoding='utf8')


def test_pagelist():
    pagelist=[]
    pagelist.append([0,40])
    # pagelist.append([40, 60])

    # for i in range(40,60,20):
    #     pagelist.append([i,i+20])
    # for i in range(60,200,10):
    #     pagelist.append([i,i+10])
    # for i in range(200,250,25):
    #     pagelist.append([i,i+25])
    # for i in range(250,600,50):
    #     pagelist.append([i,i+50])
    # for i in range(600,3000,400):
    #     pagelist.append([i,i+400])
    # for i in range(3000,5000,2000):
    #     pagelist.append([i,i+2000])
    return pagelist


ua=UserAgent()
headers = { 'User-Agent':ua.random}

# districts=['jiangbei','yubei','shapingba','jiulongpo','nanan','dadukou','banan','beibei']
districts_test=['beibei']

def pd_to_sql(dataframe,table_name,engine,if_exists_method='append',index_true_or_false=False):
    dataframe.to_sql(f'{table_name}', con=engine, if_exists=f'{if_exists_method}', index=index_true_or_false)

def get_total_fang_nums(headers,bp,ep,district,num_per_page):
    url=f'https://cq.lianjia.com/ershoufang/{district}/bp{bp}ep{ep}/'

    resp=requests.get(url,headers = headers)
    time.sleep(0.5)

    #首先查询共找到的房源套数
    find_houses_nums=int(Selector(resp).css('h2.total.fl span::text').extract_first())
    if find_houses_nums ==0:
        return 0
    #每页为30条数据
    elif find_houses_nums%num_per_page !=0:
        return find_houses_nums//num_per_page+1
    else:
        return find_houses_nums//num_per_page


def get_response_from_district_pg_bp_ep_(district,pg,bp,ep,headers):
    url=f'https://cq.lianjia.com/ershoufang/{district}/pg{pg}bp{bp}ep{ep}/'

    response=requests.get(url,headers = headers)
    time.sleep(0.5)
    return response

def get_dataframe_ready(sel):
    #准备空dataframe，用于每页的数据存储
    houses_dataframe=pd.DataFrame(columns=('community_name','page_link','location',
                                 'total_price','unit_price','room_nums',
                                 'built_area','built_time','board_time','last_deal','loan_or_not','house_type'))
    for item in sel:
        house_info={}
        try:
            community_name,location=item.css('div.positionInfo a::text').extract()
            house_info['community_name']=community_name

            page_link=item.css('div.title a::attr(href)').extract_first()
            house_info['page_link']=page_link

            house_info['location']=location

            total_price=int(float(item.css('div.totalPrice span::text').extract_first()))
            house_info['total_price']=total_price
            unit_price=item.css('div.unitPrice span::text').extract_first()
            unit_price=int(float(re.findall('单价(\d+)元',unit_price)[0]))
            house_info['unit_price']=unit_price

            house_informations=item.css('div.houseInfo::text').extract_first().split('|')
            room_nums=house_informations[0].strip()

            house_info['room_nums']=room_nums
            built_area=int(re.match('(\d+)(\\.\d+)?平米',house_informations[1].strip()).group(1))
            house_info['built_area']=built_area

            built_time=house_informations[5].strip()
            built_time=None if '年建' not in built_time else (re.match('(\d+)年建',built_time).group(1))
            house_info['built_time']=built_time

            resp2=requests.get(page_link,headers = headers)
            time.sleep(0.5)
            response_in_link=Selector(response=resp2)

            board_time=response_in_link.css('span:contains("挂牌时间")+span::text').extract_first()
            house_info['board_time']=board_time
            last_deal=response_in_link.css('span:contains("上次交易")+span::text').extract_first()
            last_deal=None if last_deal =="暂无数据" else last_deal
            house_info['last_deal']=last_deal

            loan_or_not=response_in_link.css('span:contains("抵押信息")+span::text').extract_first().strip()

            loan_or_not=None if loan_or_not =="暂无数据" else(re.match('(\w)抵押.*',loan_or_not).group(1))
            house_info['loan_or_not']=loan_or_not

            house_type=response_in_link.css('span:contains("房屋用途")+span::text').extract_first().strip()
            house_info['house_type']=house_type
        except:
            print(f'{page_link}   获取失败！')
            return None

        houses_dataframe=houses_dataframe.append(house_info,ignore_index=True)
    return houses_dataframe


def bing_xing(pg,district,bp,ep):
    response = get_response_from_district_pg_bp_ep_(district, pg, bp, ep, headers)
    sel = Selector(response).css('li.clear.LOGVIEWDATA')
    dataframe = get_dataframe_ready(sel)
    if dataframe is None:
        print(f'{district}-pg{pg}-bp{bp}--ep{ep}-------------返回数据为空！！！')
    else:
        try:
            pd_to_sql(dataframe, district, engine, 'append', False)
            print(f'{district}-pg{pg}-bp{bp}--ep{ep}数据已写入table：{district}')
        except:
            print(f'{district}-pg{pg}-bp{bp}--ep{ep}-------------数据写入失败！！！')


def main(pages):
    for district in districts_test:
        for page in pages:
            bp = page[0]
            ep = page[1]
            pg_nums=get_total_fang_nums(headers,bp,ep,district,30)

            #引入多线程
            threads=[]
            for pg in range(1,pg_nums+1):
                thread = threading.Thread(target=bing_xing, args=(pg,district,bp,ep))
                threads.append(thread)
                thread.start()

            #引入多进程

if __name__ == '__main__':
    main(test_pagelist())
