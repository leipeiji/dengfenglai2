from ..M_functionToolKit import *
import pandas as pd
from bs4 import BeautifulSoup as bf
import re
import pendulum
from  selenium import webdriver
from urllib.parse import quote
from datetime import datetime
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
class getNewProduct(object):
    def __init__(self,interval_days=5,is_save_database=1,is_save_excel=1,save_dir=r'D:\OneDrive\工作生活-总表\B3数据分析总表\1行业类目数据分析\看店宝新品采集',):

        self.save_dir =save_dir # excel or csv save path
        self.get_date=pendulum.today().date() # get now date ,eg: 2018-01-23
        self.is_save_database = is_save_database  # save  mysql  database switch ,1 means save ，0 menas not save ，the same down
        self.is_save_excel = is_save_excel  # save  excel switch
        self.database = 'taobao' # database name
        self.tableName = 'taobao_new_product_data_from_kandianbao' # table name
        # save mysql database field
        self.database_field_list = ['get_date', 'orderNum', 'firstDateAdded', 'title', 'price', 'discountPrice','create_date_char'
        ,  'sellCount', 'stock', 'favorite', 'subCategoryName','create_date', 'create_time'
            , 'itemId', 'itemUrl', 'shopWangWang', 'operator']
        # save excel title
        self.excel_title_list = ['get_date', '序号', '首次上架日期', '宝贝标题', '原价', '折扣价',
                            '宝贝创建时间', '销量', '库存', '收藏', '类目', '创建日期',
                            '创建时间', 'itemId', 'itemUrl','店铺名', '操作者']

        self.driver = self.login_kandiaobao() # login website
        self.conn=self.conn_database() # connect database from mysql use sqlalchemy
        self.shopWangWangs = self.get_shopWangwang_from_mysql()  # shop list ,eg: ['shop1','shop2']
        self.retry_count=0
        self.interval_days=interval_days # 定义本次请求和上次间隔的时间


    def conn_database(self):
        ''' connect database'''
        return con_mysql(self.database)


    def login_kandiaobao(self):
        '''login website use webdriver'''
        driver=webdriver.Chrome()
        itemUrl = 'https://my.kandianbao.com/user/login/?next=https://www.kandianbao.com/'
        driver.get(itemUrl)
        myFormat('登录时间')
        username = driver.find_element_by_id('account')
        username.send_keys('306812519@qq.com')
        waitTime2(1)
        password = driver.find_element_by_css_selector('#password')
        password.send_keys('qwert2016')

        btn = driver.find_element_by_css_selector('.sub-btn')
        btn.click()
        waitTime2(5) # 这里用百度识别验证码 判断如果有验证码
        myFormat('等待页面加载',symbol='.')
        waitTime2(2, 3)
        return driver

    def get_shopWangwang_from_mysql(self, start=0,end=None):
        sql='SELECT shopWangwang FROM `taobao_attention_shop_info` WHERE specialNote in ("自己监控店铺","粗放店铺" ) GROUP BY shopWangwang'
        df = read_sql_use_pd2(self.conn, sql)
        if end is not None:
            df = df.loc[start:end]
        if  df.empty:
            myFormat('没有查询到单品')
            return None
        # print(type(df['shopWangwang']),df['shopWangwang'])
        shopWangwangs=list(df['shopWangwang'])
        print('共操作的店铺数',len(shopWangwangs))
        print(shopWangwangs)
        return shopWangwangs


    def get_new_product_from_kandianbao(self,shopWangWang,is_refresh=0):
        ''' get one shop new product data ,save database or excel '''
        try:

            shopWangWang_encode = quote(shopWangWang) #  convert url encoed
            self.driver.get('https://dian.kandianbao.com/new/{}/'.format(shopWangWang_encode))
            waitTime2(10, 15)

            myFormat('等待数据加载',symbol='.')

            html=self.driver.page_source
            soup=bf(html,'lxml')
            itemIds=soup.find_all('a', {'class': 'hint--bottom'})[1:]
            itemIds=[ int(re.search(r'(\d+)',x.get('href')).group(1) ) for x in itemIds]
            itemUrls = ['https://detail.tmall.com/item.htm?id={}'.format(x) for x in itemIds]
            create_datetime_list=soup.select('td.created span.hint--info')
            create_dates=[ x.get('data-hint').split(' ')[0] for x in create_datetime_list]
            create_times = [x.get('data-hint').split(' ')[1] for x in create_datetime_list]
            dfs=pd.read_html(html, header=0,attrs={'class': 'table-striped'})
            print('='*100)
            df2 = pd.DataFrame()  # creates a new dataframe that's empty
            for df in dfs[0:1]:
                df2 = df2.append(df, ignore_index=True)  # 依次行追加 类似于 concat

            # df2.drop([1,2,3,4,5],inplace=True) today = pendulum.today().date()
            # df2.dropna(axis=0,how='any')
            # del df2['图 ']
            df2.drop(['图 ','分析 ','宝贝上架时间 '], axis=1, inplace=True)
            try:
                df2['折扣价 ']=df2['折扣价 '].map(lambda x: x.replace('-','-1') if  '-' in x else x)
            except:
                pass
            df2.insert(0,'get_date',self.get_date)
            df2['create_date']=create_dates
            df2['create_time']=create_times
            df2['itemId']=itemIds
            df2['itemUrl']=itemUrls
            df2['shopWangWang']=shopWangWang

            df2['operator'] = OPERATOR_NAME
            print(df2.head(2))
            if self.is_save_database:
                self.to_database(df2,shopWangWang)
            if self.is_save_excel:
                self.to_excel(df2,shopWangWang)
            waitTime2(1,3)
            self.retry_count = 0
        except Exception as e:
            print(e)
            self.retry_count+=1
            if self.retry_count<4:
                myFormat('出错，正在进行第【%d】次尝试'%self.retry_count,symbol='.')
                return self.get_new_product_from_kandianbao(shopWangWang,is_refresh=1)
            else:
                self.retry_count=0

                return None


    def check_repeat(self,itemId):
        ''' check repeat record '''
        sql = 'SELECT autoIndex from `%s`  WHERE  itemId="%s" LIMIT 1' % (self.tableName, itemId)
        print(sql)
        df = read_sql_use_pd2(self.conn, sql)
        if df.empty:  # 第一次入数据库，之前没有保存过该类目，
            return True
        else:  # 已经保存过，无需保存 ，
            return False

    def to_database(self,df_result,shopWangWang):
        ''' save mysql database '''
        df_result.columns=self.database_field_list
        for index, row in df_result.iterrows():
            itemId = int(row['itemId'])
            sellCount=int(row['sellCount'])
            stock=int(row['stock'])
            favorite=int(row['favorite'])
            firstDateAdded=row['firstDateAdded']
            create_date=row['create_date']
            create_date = '{} 0:0:0'.format(create_date)
            year = int(create_date.split('-')[0])
            month, day = re.search(r'(\d+)月(\d+)日',firstDateAdded).groups()
            temp_date = '{}-{}-{} 23:59:59'.format(year, month, day)
            if temp_date < create_date:
                year = year + 1
                temp_date = '{}-{}-{} 23:59:59'.format(year, month, day)

            firstDateAdded = temp_date.split(' ')[0]

            if not self.check_repeat(itemId):
                sql='update  `%s` set get_date="%s", firstDateAdded="%s" , create_date_char="%s",sellCount="%d",stock="%d",favorite=%d  WHERE ' \
                    ' itemId=%d' % (self.tableName,self.get_date,firstDateAdded,row['create_date_char'],sellCount,stock,favorite, itemId)
                self.conn.execute(sql)
                myFormat('产品ID【%s】店铺【%s】,更新成功！'%(itemId,shopWangWang))
                df_result.drop(axis=0,labels=[index],inplace=True)
        if df_result.empty:
            myFormat('没有要保存的数据')
        else:
            con_database_from_pd2(self.conn, self.database, self.tableName, df_result)
            myFormat('店铺【%s】--保存数据库成功！'%(shopWangWang))

    def to_excel(self,df_result,shopWangWang):
        ''' save excel '''
        try:
            if not os.path.exists(self.save_dir):
                try:
                    os.makedirs(self.save_dir)
                except:
                    raise FileNotFoundError
        except Exception as e:
            self.save_dir = r'{}\{}'.format(os.path.dirname(__file__), '新品')
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        outputfile =r'{a}\{b}_{c}_{d}.xlsx'.format(a=self.save_dir, b=self.get_date, c=shopWangWang, d='新品')
        df_result.to_excel(outputfile, index=False)
        myFormat('店铺【%s】--保存excel成功！' % (shopWangWang))
        myFormat('保存地址【%s】'%(outputfile))



    def get_many_shop_new_product(self):
        ''' walking down the many shop list  '''
        total=len(self.shopWangWangs)
        for k, shopWangWang in enumerate(self.shopWangWangs,1):
            myFormat('正在处理 【%d/%d】 -- 进度【%s】 --【%s】'%(k,total,display_percent(k/total),shopWangWang),symbol='.')
            try:
                # 查询上次保存的日期
                sql='SELECT get_date FROM `taobao_new_product_data_from_kandianbao`' \
                    ' WHERE shopWangWang="{shopWangWang}" ORDER BY get_date DESC LIMIT 1 '.format(shopWangWang=shopWangWang)
                if not check_last_save_interval_days(self.conn, sql, interval_days=3, date_field_name='get_date'): continue

                self.get_new_product_from_kandianbao(shopWangWang)
            except Exception as e:
                myFormat('可能该店铺没上新产品')
                text = '【%s】店铺出错-- 【%s】' % (shopWangWang, e)
                my_logging(text, filename=os.path.basename(__file__))
                continue
        else:
            self.driver.close()
            mysql_close(self.conn)

#
# new=getNewProduct(is_save_database=1,is_save_excel=1)
# new.get_many_shop_new_product()

