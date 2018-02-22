# -*- coding: utf-8 -*-

from ..config import *
from ..M_functionToolKit import *
import re, requests
import numpy as np
import pandas as pd
from gevent import monkey
import gevent
from bs4 import BeautifulSoup as bf
import hashlib


# md5=hashlib.md5('字符串'.encode('utf-8')).hexdigest()
# print(md5)
# monkey.patch_all()

class baiduNews(object):
    def __init__(self, is_save_excel=0, is_save_database=1):
        self.retry_count = 0
        self.retry_count2 = 0
        self.get_date = time.strftime("%Y-%m-%d", time.localtime())
        self.is_save_excel = is_save_excel  # 保存excel 开关，1 保存 0不保存
        self.is_save_database = is_save_database  # 保存数据库开关
        self.database = 'peiji'
        self.tableName = 'baidu_news'
        self.database_field_list = ['get_date', 'type', 'orderNum', 'keyword', 'url', 'hash_md5', 'operator']
        self.excel_title_list = ['日期', '类型', '序号', '关键词', '链接', 'hash_md5', '操作者']
        self.SaveDir = r'{}:\综合信息\百度新闻'.format(ROOT_DIR)

    def get_seven_attention_from_baidu(self,):

        try:
            RequestsUrl = 'http://top.baidu.com/buzz?b=42&fr=topindex'
            headers = {'User-Agent': random.choice(user_agent_list),
                       # 'Cookie':'l=AjQ0YH4ahegCkegQQS-ojotThPikLlj3; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; ali_ab=223.88.24.116.1468809802043.4; UM_distinctid=15fb42daf03309-0746c6a3b3e69-c303767-1fa400-15fb42daf04680; mt=np=&ci=-1_0; tg=0; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; cna=iXD9Dm7atRMCAasIkleQfjds; _m_h5_tk=0ac02245b626b389c76a41d268236c0c_1512369105101; _m_h5_tk_enc=9872d2f3e8b345c968a1f2baa22ac1bb; cookie2=2de271544b9a239ce548083ead0c13b9; v=0; _tb_token_=f86593e5beb7b; ockeqeudmj=mpcmsm0%3D; munb=2202492619; WAPFDFDTGFG=%2B4cMKKP%2B8PI%2BuWT85ud5R2V%2BCWGXQaqSD6SsYg%3D%3D; _w_app_lg=18; unb=2202492619; sg=097; t=a927dbf5a9f4945bd2d09b5465989e51; _l_g_=Ug%3D%3D; skt=89dce6ce5ed50bfd; uc1=cookie21=UtASsssme%2BBq&cookie15=U%2BGCWk%2F75gdr5Q%3D%3D&cookie14=UoTdeYfMVHjvXQ%3D%3D; cookie1=V3pJUy9O3KFtsrxVhLDMvsPN%2FLaA6G5sWxcGibE2JeQ%3D; uc3=vt3=F8dBzLQKaYjMSNvETSo%3D&id2=UUphyIyHMmDwAQ%3D%3D&nk2=rPh94HZsm%2Ft55rZx3Do%3D&lg2=WqG3DMC9VAQiUQ%3D%3D&sg2=BxZpTCsBBLErxMus9UHPoSuHMxT1i8PODTiSPRYnQCs%3D; tracknick=%5Cu5411%5Cu65E5%5Cu847520141010; uss=AnWd7gg2ghS0ERrJVUZTx0glmYRxNqJkkkPBlj%2BgkY5dXazye3cu2qkXDWo%3D; lgc=%5Cu5411%5Cu65E5%5Cu847520141010; _cc_=W5iHLLyFfA%3D%3D; _nk_=%5Cu5411%5Cu65E5%5Cu847520141010; cookie17=UUphyIyHMmDwAQ%3D%3D; ntm=0; isg=AujoR4eYJq87BweDEEAFPNMrudY6uU1voitNWKIZNGNW_YhnSiEcq34_h6_y',
                       }
            proxies = {
                "https": "116.31.124.104:3128",
                "http": "27.152.7.18:808", }
            # RequestsUrl='https://www.baidu.com/'
            r = requests.get(RequestsUrl, headers=headers)
            r.encoding = 'gb2312'
            print(r.status_code, r.url)
            if r.status_code == 200:
                # print(r.text)
                soup = bf(r.text)
                seven_attention_list = soup.select('.list-table tr')[1:]
                result_list = []
                for k, ls in enumerate(seven_attention_list):
                    keyword = ls.select('.keyword a')[0].text.strip()
                    hash_md5 = hashlib.md5(keyword.encode('utf-8')).hexdigest()
                    if not self.check_repeat(keyword=hash_md5):
                        myFormat('关键词 【%s】 已经存在了，跳过' % keyword)
                        continue
                    url = ls.select('.keyword a')[0].get('href')
                    print(keyword, url)
                    result_list.append([self.get_date, 1, k + 1, keyword, url, hash_md5, OPERATOR_NAME])
                print(len(result_list))
                # pprint( result_list)
                data_dict = dict(zip(range(len(result_list)), result_list))
                df = pd.DataFrame(data_dict)
                df_T = df.T
                print(df_T.head())
                return df_T
        except Exception as e:
            print(e, )
            self.retry_count += 1
            if self.retry_count < 3:
                waitTime(self.retry_count * 3)
                return self.get_seven_attention_from_baidu()
            else:
                self.retry_count = 0
                print('max error')
                return None

    # 头条新闻列表
    def get_hot_news_from_baidu(self,):

        try:
            RequestsUrl = 'http://news.baidu.com/'
            headers = {'User-Agent': random.choice(user_agent_list),
                       'Referer': 'https://www.baidu.com/',
                       # 'Cookie':'l=AjQ0YH4ahegCkegQQS-ojotThPikLlj3; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; ali_ab=223.88.24.116.1468809802043.4; UM_distinctid=15fb42daf03309-0746c6a3b3e69-c303767-1fa400-15fb42daf04680; mt=np=&ci=-1_0; tg=0; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; cna=iXD9Dm7atRMCAasIkleQfjds; _m_h5_tk=0ac02245b626b389c76a41d268236c0c_1512369105101; _m_h5_tk_enc=9872d2f3e8b345c968a1f2baa22ac1bb; cookie2=2de271544b9a239ce548083ead0c13b9; v=0; _tb_token_=f86593e5beb7b; ockeqeudmj=mpcmsm0%3D; munb=2202492619; WAPFDFDTGFG=%2B4cMKKP%2B8PI%2BuWT85ud5R2V%2BCWGXQaqSD6SsYg%3D%3D; _w_app_lg=18; unb=2202492619; sg=097; t=a927dbf5a9f4945bd2d09b5465989e51; _l_g_=Ug%3D%3D; skt=89dce6ce5ed50bfd; uc1=cookie21=UtASsssme%2BBq&cookie15=U%2BGCWk%2F75gdr5Q%3D%3D&cookie14=UoTdeYfMVHjvXQ%3D%3D; cookie1=V3pJUy9O3KFtsrxVhLDMvsPN%2FLaA6G5sWxcGibE2JeQ%3D; uc3=vt3=F8dBzLQKaYjMSNvETSo%3D&id2=UUphyIyHMmDwAQ%3D%3D&nk2=rPh94HZsm%2Ft55rZx3Do%3D&lg2=WqG3DMC9VAQiUQ%3D%3D&sg2=BxZpTCsBBLErxMus9UHPoSuHMxT1i8PODTiSPRYnQCs%3D; tracknick=%5Cu5411%5Cu65E5%5Cu847520141010; uss=AnWd7gg2ghS0ERrJVUZTx0glmYRxNqJkkkPBlj%2BgkY5dXazye3cu2qkXDWo%3D; lgc=%5Cu5411%5Cu65E5%5Cu847520141010; _cc_=W5iHLLyFfA%3D%3D; _nk_=%5Cu5411%5Cu65E5%5Cu847520141010; cookie17=UUphyIyHMmDwAQ%3D%3D; ntm=0; isg=AujoR4eYJq87BweDEEAFPNMrudY6uU1voitNWKIZNGNW_YhnSiEcq34_h6_y',
                       }
            proxies = {
                "https": "182.90.78.151:8123",
                "http": "113.205.10.28:8998", }
            # RequestsUrl='https://www.baidu.com/'
            r = requests.get(RequestsUrl, headers=headers)
            # r.encoding = 'gb2312'
            print(r.status_code, r.url)
            if r.status_code == 200:
                # print(r.text)
                soup = bf(r.text)
                hot_news_list = soup.select('#pane-news ul li')
                result_list = []
                for k, ls in enumerate(hot_news_list):
                    link_list = ls.select('a')
                    # print(len(link_list))
                    for i, innerls in enumerate(link_list):
                        try:
                            title = innerls.text.strip()
                            # myFormat(len(title))
                            hash_md5 = hashlib.md5(title.encode('utf-8')).hexdigest()
                            if not self.check_repeat(title=hash_md5):
                                myFormat('关键词 【%s】 已经存在了，跳过' % title)
                                continue
                            url = innerls.get('href')
                            print(title, url)
                            result_list.append(
                                [self.get_date, 2, ((k + 1) + (i + 0)), title, url, hash_md5, OPERATOR_NAME])
                        except Exception as e:
                            myFormat('error skip')
                            continue
                    time.sleep(0.1)
                print(len(result_list))
                # pprint( result_list)
                data_dict = dict(zip(range(len(result_list)), result_list))
                df = pd.DataFrame(data_dict)
                df_T = df.T
                print(df_T.head())
                return df_T
        except Exception as e:
            print(e, )
            self.retry_count2 += 1
            if self.retry_count2 < 4:
                waitTime(self.retry_count2 * 5)
                return self.get_hot_news_from_baidu()
            else:
                self.retry_count2 = 0
                print('max error')
                return None

    # 保存到excel
    def toExcel(self, df):
        df.columns = self.excel_title_list

        try:
            if not os.path.exists(self.SaveDir):
                try:
                    os.makedirs(self.SaveDir)
                except:
                    raise FileNotFoundError
        except Exception as e:
            SaveDir = r'{}\{}'.format(os.path.dirname(__file__), '百度新闻')
        if not os.path.exists(SaveDir):
            os.makedirs(SaveDir)
        outputfile = '{a}\{e}_{f}_{g}.xlsx'.format(a=SaveDir, e=self.get_date, f='新闻', g=random.randint(1000, 9999))
        df.to_excel(outputfile, index=False, sheet_name='data')
        print('保存excel成功，路径在\t%s' % outputfile)

    # 保存到数据库
    def toDatabase(self, df):
        df.columns = self.database_field_list
        con_database_from_pd(self.database, self.tableName, df)
        myFormat('数据库名【%s】,表名是【%s】' % (self.database, self.tableName))

    # 检查重复关键词新闻
    def check_repeat(self, keyword=None, title=None):
        type = 1 if keyword else 2
        word = keyword if keyword else title
        sql = 'SELECT autoIndex from `%s`  WHERE hash_md5="%s" and type=%d LIMIT 1' % (self.tableName, word, type)
        print(sql)
        df = read_sql_use_pd(self.database, sql)
        if df.empty:  # 第一次入数据库，之前没有保存过该类目，
            return True
        else:  # 已经保存过，无需保存 ，
            return False

    def main_baidu_news(self):
        myFormat('正在获取 【%s】七日关注列表' % (self.get_date))

        df1 = self.get_seven_attention_from_baidu()
        if df1.empty:
            myFormat('七天关注数据，所有关键词都已经存在了')
        else:
            myFormat('开始保存七天关注数据', symbol='.', fillMode='right')
            self.toDatabase(df1)

        df2 = self.get_hot_news_from_baidu()
        if df2.empty:
            myFormat('top新闻数据，所有关键词都已经存在了')
        else:
            myFormat('开始保存top新闻数据', symbol='.', fillMode='right')
            self.toDatabase(df2)

        # if is_save_database:
        #     myFormat('开始保存七天关注数据',symbol='.',fillMode='right')
        #     toDatabase(df1)
        #     myFormat('开始保存top新闻数据',symbol='.',fillMode='right')
        #     toDatabase(df2)
        if self.is_save_excel:
            df = pd.concat([df1, df2])
            self.toExcel(df, self.SaveDir)
        myFormat('获取完成')

# ╭~~~╮
# (o^.^o)
# ------ 功能 -读取百度新闻 top板块列表 http://news.baidu.com/  ,每天可以更新，会自动去重  ------
# ------ 读取 百度排行榜 七日 关注热词  ，一次同时抓取这2个  ------
# ------ type=1 表示 七日 关注热词，2 表示 百度新闻 top板块列表 ----
# ------ 对新闻标题 采用了 简单的 md5加密，主要是为了 用数据库检查去重时，检索速度更快些，直接检索汉字，比较慢，统一转化成32位的字符串，会好些
# ╭~~~╮
# (o^.^o)



