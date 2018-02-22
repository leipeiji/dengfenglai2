# -*- coding: utf-8 -*-
from ..config import *
from ..M_functionToolKit import *
import re
import numpy as np
import pandas as pd
# -*- coding:utf-8 -*-
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bf
import hashlib
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from selenium import webdriver
from time import sleep
# from pyvirtualdisplay import Display

class shopFans(object,):

    def __init__(self,is_save_excel=0,is_save_database=1,wait_time=1,is_multi_process=0,save_dir=r'D:\OneDrive\工作生活-总表\B3数据分析总表\1行业类目数据分析\监控店铺粉丝'):
        self.save_dir = save_dir
        self.database = 'taobao'
        self.tableName = 'taobao_shop_fans'
        self.is_save_excel = is_save_excel
        self.is_save_database = is_save_database
        self.is_multi_process = is_multi_process
        self.wait_time = wait_time  # 获取两个产品之间的等待时间
        self.get_date = time.strftime("%Y-%m-%d", time.localtime())  # 不用修改
        self.database_field_list = ['get_date', 'shopWangwang', 'fans_counter', 'fans_counter_num', 'shop_url', 'shopId',
                               'operator']
        self.excel_title_list = ['查询时间', '店铺旺旺', '粉丝数', '粉丝数-数字形式', '店铺地址', '店铺Id', '操作者']
        self.conn=con_mysql(self.database)
    def check_repeat(self,shopId, get_date):
        sql = 'SELECT autoIndex from `%s`  WHERE  get_date= "%s" and  shopId=%d  LIMIT 1' % (self.tableName, get_date, shopId)
        print(sql)
        df = read_sql_use_pd(self.database, sql)
        if df.empty:  # 第一次入数据库，之前没有保存过该类目，
            return True
        else:  # 已经保存过，无需保存 ，
            return False


    def to_excel(self,result, get_date):
        result.columns = self.excel_title_list
        try:
            if not os.path.exists(self.save_dir):
                try:
                    os.makedirs(self.save_dir)
                except:
                    raise FileNotFoundError
        except Exception as e:
            self.save_dir = r'{}\{}'.format(os.path.dirname(__file__), '店铺粉丝')
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        outputfile = '{a}\{e}_{f}.xlsx'.format(a=self.save_dir, e=get_date, f='店铺粉丝')

        result.to_excel(outputfile, index=False, sheet_name='data')
        print('保存excel成功，路径在\t%s' % outputfile)


    def to_database(self,df_data):
        if self.is_save_database and (not df_data.empty):
            df_data.columns = self.database_field_list
            # df_data.drop_duplicates(subset=['hash_md5'], keep='first', inplace=True)
            con_database_from_pd2(self.conn,self.database, self.tableName, df_data)


    def get_shop(self,):
        sql='SELECT shopWangwang,shopUrl FROM `taobao_attention_shop_info` WHERE specialNote="自己监控店铺" and shopRank="天猫" GROUP BY shopWangwang;'
        df=read_sql_use_pd2(self.conn,sql=sql)
        df['shopId']= df['shopUrl'].map(lambda x:re.search(r'(\d+)',x).group(1))

        df['wx_shop_url']= df['shopUrl'].map(lambda x:'https://shop.m.taobao.com/shop/shop_index.htm?shop_id={}'
                                                      ''.format(re.search(r'(\d+)',x).group(1)))

        results=[ (x,int(y),z,m) for x,y ,z,m in  zip(df['shopWangwang'],df['shopId'],df['wx_shop_url'],df['shopUrl'])]
        pprint(results)
        return results


    def process_many_shop(self,get_date):
        WIDTH = 320
        HEIGHT = 640
        PIXEL_RATIO = 3.0
        UA = random.choice(user_agent_phone)
        mobileEmulation = {"deviceMetrics": {"width": WIDTH, "height": HEIGHT, "pixelRatio": PIXEL_RATIO}, "userAgent": UA}
        options = webdriver.ChromeOptions()
        options.add_experimental_option('mobileEmulation', mobileEmulation)
        driver = webdriver.Chrome( chrome_options=options)
        # driver.get('https://mitangzhicheng.m.tmall.com/shop/shop_auction_search.htm?suid=2095767659&sort=default')
        # 下面可以弄成字典的形式  一个店铺名 一个连接
        # shop_link_list=['http://shop114249705.m.taobao.com','http://shop103991552.m.taobao.com','https://esey.m.tmall.com/','https://mitangzhicheng.m.tmall.com/','https://msuya.m.tmall.com/?shop_id=66459281']
        results=self.get_shop()
        result_list=[]
        for shopWangwang,shopId,wx_shop_url ,shop_url in tqdm(results):
            if not self.check_repeat(shopId, get_date):
                myFormat('店铺【%s】在【%s】 已经保存过了，跳过'%(shopWangwang,get_date))
                continue
            driver.get(wx_shop_url)
            myFormat('正在处理店铺【%s】'%shopWangwang,symbol='.',fillMode='right')
            # print(driver.page_source)
            try:
                soup = bf(driver.page_source)
                fans_counter = soup.select('.collect-counter')[0].text.strip()
                temp = round(float(re.search(r'(\d+\.*\d*)', fans_counter).group(1)), 3)
                fans_counter_num= int(temp*10000)  if '万' in fans_counter else int(temp)
            except:
                # print(driver.page_source)
                # fans_counter_num=soup.select('div#shop_mod_container span')
                # print(fans_counter_num)
                fans_counter=-1
                fans_counter_num=-1
                # exit()
            print(shopWangwang, fans_counter)  # 33.3万
            data_list=[[get_date,shopWangwang,fans_counter,fans_counter_num,shop_url,shopId,OPERATOR_NAME]]
            data_dict = dict(zip(range(len(data_list)), data_list))
            df = pd.DataFrame(data_dict)
            df_T = df.T
            if self.is_save_database:
                self.to_database(df_T)
            result_list.append(df_T)
            waitTime2(1,3)
        else:
            df_results = pd.concat(result_list)
            if df_results.empty:
                myFormat('没有要保存的数据')
                return None

            print(df_results.head(5))
            if self.is_save_excel:
                self.to_excel(df_results, get_date, self.save_dir)


            driver.close()
            # display.stop()
            # 这里封装下 可以获取店铺的 粉丝数 http://shop103991552.taobao.com pc端这个地址 变成  http://shop103991552.m.taobao.com 就行了，构造下
            # 需要的话 可以加上 切换IP 就是用代理ip
            # 店铺名 店铺链接 shopid

    def multi_process(self,result):

        WIDTH = 320
        HEIGHT = 640
        PIXEL_RATIO = 3.0
        UA = random.choice(user_agent_phone)

        mobileEmulation = {"deviceMetrics": {"width": WIDTH, "height": HEIGHT, "pixelRatio": PIXEL_RATIO}, "userAgent": UA}
        options = webdriver.ChromeOptions()
        options.add_experimental_option('mobileEmulation', mobileEmulation)
        # 下面代码 不需要打开 chrome浏览器 运行 2行代码
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(chrome_options=options)
        # driver.get('https://mitangzhicheng.m.tmall.com/shop/shop_auction_search.htm?suid=2095767659&sort=default')
        # 下面可以弄成字典的形式  一个店铺名 一个连接
        # shop_link_list=['http://shop114249705.m.taobao.com','http://shop103991552.m.taobao.com','https://esey.m.tmall.com/','https://mitangzhicheng.m.tmall.com/','https://msuya.m.tmall.com/?shop_id=66459281']

        shopWangwang, shopId, wx_shop_url, shop_url=result
        if not self.check_repeat(shopId, self.get_date):
            myFormat('店铺【%s】在【%s】 已经保存过了，跳过' % (shopWangwang, get_date))
            return pd.DataFrame({})
        driver.get(wx_shop_url)
        myFormat('正在处理店铺【%s】' % shopWangwang, symbol='.', fillMode='right')
        # print(driver.page_source)
        try:
            soup = bf(driver.page_source)
            fans_counter = soup.select('.collect-counter')[0].text.strip()
            temp = round(float(re.search(r'(\d+\.*\d*)', fans_counter).group(1)), 3)
            fans_counter_num = int(temp * 10000) if '万' in fans_counter else int(temp)
        except:
            # print(driver.page_source)
            # fans_counter_num=soup.select('div[data-role="shop_head"] span')
            # print(fans_counter_num)

            fans_counter = -1
            fans_counter_num = -1
        print(shopWangwang, fans_counter)  # 33.3万
        data_list = [[self.get_date, shopWangwang, fans_counter, fans_counter_num, shop_url, shopId, OPERATOR_NAME]]
        data_dict = dict(zip(range(len(data_list)), data_list))
        df = pd.DataFrame(data_dict)
        driver.close()
        return df.T
        # if is_save_database:
        #     to_database(df_T)
    def main(self):
        if self.is_multi_process:
            my_pool = Pool()
            results = self.get_shop()
            df_result = my_pool.map(self.multi_process, results)
            my_pool.close()
            my_pool.join()
            new_df = pd.concat(df_result)
            print(new_df)
            if self.is_save_database:
                self.to_database(new_df)
            if self.is_save_excel:
                self.to_excel(new_df, self.get_date, self.save_dir)
        else:
            self.process_many_shop(self.get_date)






