# -*- coding: utf-8 -*-
from ..config import taobao_all_category_reversed,ROOT_DIR,user_agent_list,OPERATOR_NAME,headers
from ..M_functionToolKit import my_logging,myFormat,con_database_from_pd,from_dir_get_allfile_recursion
import requests
from openpyxl import Workbook
import math
import random
import json
import time
import pandas as pd
import tqdm
from pprint import pprint
import os
# ---------------------功能--------- 获取 品牌团 全部店铺  ----------更新时间  2017-08-20 --------------
#==================  主程序开始====================

# info:获取聚划算所有类目 品牌团 销量 开团提醒 入口类目图等信息
#authou: ----等风来----
#create: 2017-08-28

# allCatID ={261000: '女装饰品',}
# allCatID ={268000: '食品百货'}
#https://ju.taobao.com/json/tg/ajaxGetBrandsV2.json?
# page=1&_ksTS=1503884889105_299&callback=brandList_0&page=1&btypes=1,2&showType=0&frontCatIds=261000&salesSites=1
#不同的 frontCatIds 控制着全局的分类 比如是女装还是家电, 每个分类下 页码不一样，要先读取页码 ，然后可以多线程 每个线程不同的就是 分类的ID 分别保存在不同的文件夹中

class JuBrand(object):
    def __init__(self,save_dir=r'{}:\聚划算'.format(ROOT_DIR),is_save_excel=0,
                 is_save_database=1,keyWord = '品牌团_',
                 temp_keyWord = '',):
      
        self.allCatID = {261000: '女装饰品', 262000: '精品男士', 264000: '鞋类箱包', 265000: '内衣配饰', 303000: '运动户外',
                    266000: '母婴童装', 267000: '美容护肤', 268000: '食品百货', 269000: '数码家电', 270000: '家装车品'}
        self.database = 'taobao'
        self.tableName = 'taobao_juhuasuan_brand_brief'
        self.database_field_list = ['page', 'crawlDate', 'startTime', 'endTime', 'onlineItemCount',
                                'brandName', 'soldCount', 'remindNum', 'timeRemind', 'sellerId', 'brandId',
                               'shopId', 'rootCategoryId', 'rootCategoryName', 'activityUrl', 'brandEnterImgUrl',
                               'promotion', 'benefitText', 'brandDesc', 'brandCategory', 'operator']

        self.is_save_database = is_save_database
        self.is_save_excel = is_save_excel
        self.temp_keyWord = temp_keyWord  # 在大活动时，每天保存多次数据，用的比较多 这里区分下目录 和文件名
        self.keyWord = keyWord + self.temp_keyWord
        self.save_dir = save_dir
        self.is_save_pic = 0  # 是否保存图片开关  1是保存 0 不保存
        self.firstPath = '{}\{}\{}'.format(self.save_dir, time.strftime("%Y-%m-%d", time.localtime()), self.keyWord)
        if not os.path.exists(self.firstPath):
            os.makedirs(self.firstPath)


    def getAllJuListAjax(self,frontCatIds,p=1, RequestsUrl='https://ju.taobao.com/json/tg/ajaxGetBrandsV2.json'):

        try:
              tempNum = random.randint(200, 999)
              callback='brandList_0'
              query = {'page': p,'_ksTS': str(round(time.time() * 1000)) + '_' + str(tempNum),
                       'callback':callback,
                       # 'btypes':'1,2',
                       'showType':0, # 1表示 预告 还没有开团
                       'frontCatIds':	frontCatIds,
                       # 女装饰品 261000,精品男士262000,鞋类箱包 	264000，内衣配饰 265000 ，运动户外 	303000
                       # 母婴童装 266000,美容护肤 267000，食品百货 	268000,数码家电 269000,家装车品 270000,即将上线 261000
                       'salesSites':1}
              headers = {'user-agent': random.choice(user_agent_list)}
              r = requests.get(RequestsUrl, params=query, headers=headers)
              print(r.status_code,r.url)
              if r.status_code==200:
                  c = r.text.replace('brandList_0(','').replace(')','')
                  new_json = json.loads(c)
                  # pprint(new_json)
              self.retry_count = 0
              return new_json
        except:
            self.retry_count+=1
            if self.retry_count<3:
                text = 'getAllJuListAjax,第%d次链接出现问题，正在请求第%d次链接......' % (self.retry_count,self.retry_count + 1)
                my_logging(text, filename='M_juBrand.log')
                print(text)
                return self.getAllJuListAjax(frontCatIds, p=p, RequestsUrl='https://ju.taobao.com/json/tg/ajaxGetBrandsV2.json')
            else:
                self.retry_count=0
                myFormat('error stop，出错达到最大次数，停止',symbol='☹')
                return None


    #获取每一页的所有品牌
    #目前测试阶段 先不加异常处理 看看会发生什么问题
    def getEveryJuInfo(self,frontCatIds):
        try:
            jsonList = self.getAllJuListAjax(frontCatIds, p=1)
            totalPage=int(jsonList['totalPage'])
            totalNum=jsonList['totalNum']
            allPageInfoList=[]
            print('总计%d页,%d个品牌.............'%(totalPage,totalNum))
            for page in range(1,totalPage+1):
              allProduct = []
              everyPageJsonList = self.getAllJuListAjax(frontCatIds, p=page)['brandList']
              for k, js in enumerate( everyPageJsonList):
                  startTime=time.strftime('%Y-%m-%d %H:%M:%S',
                                            time.localtime(int(js.get('baseInfo').get('ostime'))/1000))
                  endTime = time.strftime('%Y-%m-%d %H:%M:%S',
                                              time.localtime(int(js.get('baseInfo').get('oetime')) / 1000))
                  brandDesc = js.get('materials').get('brandDesc',' ')
                  benefitText = js.get('materials').get('benefitText',' ')
                  promotion = js.get('price').get('promotion',' ')
                  onlineItemCount = js.get('extend').get('onlineItemCount',0)
                  sellerId=js.get('baseInfo').get('sellerId',0)
                  rootCategoryId = js.get('baseInfo').get('tbFirstCatId', 0)
                  brandId = js.get('baseInfo').get('brandId', 0)
                  shopId=js.get('extend').get('shopId',0)
                  rootCategoryName=taobao_all_category_reversed.get(int(rootCategoryId))
                  # print(js['baseInfo'].get('brandName'))
                  innerProduct = [   str(page),
                  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                    startTime,endTime,onlineItemCount,
                                    js.get('baseInfo').get('brandName'),
                                  #str(js['extend']['brandOfficialFlagShip']),
                                    str(js.get('remind').get('soldCount',0)),str(js.get('remind').get('remindNum',0))
                      ,str(js.get('remind').get('timeRemind')),
                                     int(sellerId),int(brandId),int(shopId),int(rootCategoryId),rootCategoryName,

                                    js.get('baseInfo').get('activityUrl'),
                                     'http:'+js.get('materials').get('newBrandEnterImgUrl'),
                                  promotion,benefitText,brandDesc,
                     ]
                  if k%10==0:
                      print(innerProduct)
                  # print(innerProduct)
                  allProduct.append(innerProduct)
              myFormat('第%d页%d个品牌处理完毕'%(page,len(allProduct)))
              allPageInfoList+=allProduct
            return allPageInfoList
        except Exception as e:
            myFormat(' %s error,检查getEveryJuInfo 函数'%(e),symbol='☹')
            return None

    #存到excel文件 这个用在excel 2007及以上的版本
    def toExcel(self,allPageInfoList,frontCatIds,sheetName='data'):
        if len(allPageInfoList)==0 or allPageInfoList==None:
            myFormat('error,不是有效写入数据')
            return None
        # doc = r'{}\{}.{}'.format(path, filename, 'xlsx')
        AllExcelHead = ['0页码', '1抓取时间',  '2开抢时间', '3结束时间', '4参团商品数', '5品牌', '6销量', '7开团提醒数',
                        '8剩余时间','sellerId', 'brandId', 'shopId', 'rootCategoryId', '根类目', '9团购链接', '10入口图片', '优惠','卖点','品牌介绍']
        #计算总页码
        catPage=math.ceil(len(allPageInfoList)/96)
        doc= r'{}\{}_共{}页_{}个品牌.{}'.format(self.firstPath,self.allCatID[frontCatIds],catPage,len(allPageInfoList), 'xlsx')
        try:
            # 在内存创建一个工作簿obj
            wb = Workbook()
            ws=wb.active
            #给sheet明个名
            ws.title = sheetName
            # 向第一个sheet页写数据吧 格式 ws2['B1'] = 4
            ws.append(AllExcelHead)
            for k, line in enumerate( allPageInfoList):
                try:
                    # print(line)
                    #line是一个列表 类似[1,2,3.4.5]
                    ws.append(line)
                    if k % 10 == 0:
                        print('写入第%d条记录完毕' % (k+1))
                except:
                    print('第%d条记录有问题，已经忽略' % k)
                    continue
            # if k == len(allPageInfoList):
            else: #for循环结束是执行else下面的
                print('###############恭喜你，%s完毕#####################'%self.allCatID[frontCatIds])
                # i += 1
            # 工作簿保存到磁盘
            wb.save(doc)
            print('保存文件路径是\t%s'%doc)
        except Exception as e :
            print(e,'保存工作表文件时，出现问题了，请检查 toExcel这个函数')
            pass

    def to_database(self,allPageInfoList,frontCatIds):
        brandCategory=self.allCatID[frontCatIds]
        data_dict = dict(zip(range(len(allPageInfoList)), allPageInfoList))
        df = pd.DataFrame(data_dict)
        df_T = df.T  # 转置成 正常用pd 能写入 excel和数据库的类型
        df_T['brandCategory'] = brandCategory
        df_T['operator'] = OPERATOR_NAME
        df_T.columns = self.database_field_list
        con_database_from_pd(self.database, self.tableName, df_T)
        myFormat('数据库名【%s】,表名是【%s】' % (self.database, self.tableName))
        # df_T.insert(0, 'get_date', date_range)


    #下载图片
    def downloadPic(self,allPageInfoList,frontCatIds):
        try:
            if len(allPageInfoList)==0 or allPageInfoList==None:
                myFormat('error,不是有效写入数据,无法下载图片')
                return None
            savePicPath = r'{}\{}'.format(self.firstPath,self.allCatID[frontCatIds])
            if not os.path.exists(savePicPath):
                os.makedirs(savePicPath)
            k=0
            for n in allPageInfoList:
                k += 1
                PrePic =  '序号{}_销量{}_开团提醒{}_剩余时间TT{}'.format(str(k),n[6],n[7],n[8])
                file_path = '{0}\{1}.{2}'.format(savePicPath, PrePic, 'jpg')
                try:
                    # print(n[-4],end='==============\n')
                    response = requests.get(n[-4], headers=headers)
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                        f.close()
                    if k % 10 == 0:
                        print('保存第%d张图片完毕了' %(k))
                    # if k==len(allPageInfoList):
                except Exception as e:
                    print(e,'保存图片---%s---时出错了，本张图片没有保存' %(PrePic))
                    continue
            else:
                print('##################恭喜你，%s类目图片全部保存完毕!#######################'%self.allCatID[frontCatIds])
        except Exception as e:
            myFormat('error downloadPic')
            return None

    def many_doc_operator(self,source_directory):
        doc_list=from_dir_get_allfile_recursion(source_directory,is_like=1,_like='个产品.xlsx')
        pprint(doc_list)
        for doc in tqdm(doc_list):
            try:
                filename=os.path.basename(doc)
                brandCategory=filename.split('_')[0]
                print(brandCategory)
                self.temp_save_database(doc, brandCategory)
            except Exception as e:
                print(e,'出错，跳过哦')
                continue
        else:
            myFormat('所有弄完了')

    def temp_save_database(self,doc,brandCategory):
        if doc is None:
            myFormat('不是有效的excel文件,或本店铺【%s】没有要更新的数据'%brandCategory)
            return None
        if self.is_save_database:
            df = pd.read_excel(doc,sheetname='data')
            df['brandCategory'] = brandCategory
            df['operator'] = OPERATOR_NAME
            df.columns = self.database_field_list
            # 重复值检查
            if df.empty:
                myFormat('没有要更新的的')
                return None
            con_database_from_pd(self.database, self.tableName, df)
            myFormat('数据库名【%s】,表名是【%s】' % (self.database, self.tableName))


    def main(self,frontCatIds):
        allPageInfoList = self.getEveryJuInfo(frontCatIds) #得到列表信息
        if self.is_save_excel:
            self.toExcel(allPageInfoList, frontCatIds) #写入文件
        if self.is_save_database:
            self.to_database(allPageInfoList, frontCatIds)
        if self.is_save_pic:
            self.downloadPic(allPageInfoList, frontCatIds) #下载图片



    

