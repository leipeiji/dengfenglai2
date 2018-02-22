# -*- coding: utf-8 -*-
from dengfenglai.config import *
from dengfenglai.M_functionToolKit import *
import re
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium import webdriver


class SaveWuxianHomeFullScreenCapture(object,):

    def __init__(self,wait_time=1,save_dir=r'D:\0综合信息\自己_无线首页全屏截图'):
        self.save_dir = save_dir
        self.wait_time = wait_time  # 获取两个产品之间的等待时间
        self.get_date = time.strftime("%Y-%m-%d", time.localtime())  # 不用修改
        self.database='taobao'
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--window-size=1080,1920")  # 设置窗口大小
        self.driver = webdriver.Chrome(chrome_options=chrome_options)


    def get_shop(self,):
        sql='SELECT shopWangwang,shopUrl,firstCategory FROM `taobao_attention_shop_info` WHERE  shopRank="天猫" GROUP BY shopWangwang;'
        df=read_sql_use_pd(self.database,sql=sql)
        df['shopId']= df['shopUrl'].map(lambda x:re.search(r'(\d+)',x).group(1))

        df['wx_shop_url']= df['shopUrl'].map(lambda x:'https://shop.m.taobao.com/shop/shop_index.htm?shop_id={}'
                                                      ''.format(re.search(r'(\d+)',x).group(1)))

        results=[ (x,int(y),z,m,n) for x,y ,z,m,n in  zip(df['shopWangwang'],df['shopId'],df['wx_shop_url'],df['shopUrl'],df['firstCategory'])]
        print('处理店铺数',len(results),results)
        return results


    def process_many_shop(self):
        ''' 一次保存 多个店铺   '''
        results=self.get_shop()
        total=len(results)
        print('总店铺数%d'%(total))
        k=0
        for shopWangwang,shopId,wx_shop_url ,shop_url,firstCategory in results:
            try:
                myFormat('进度【%s】--正在处理店铺【%s】' % (display_percent(k/total),shopWangwang), symbol='.')
                year, month, day = self.get_date.split('-')
                save_dir = '{}\{}\{}\{}\{}'.format(self.save_dir, firstCategory, year, month, day)
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                image_outpath = '{}\{}_{}.png'.format(save_dir, self.get_date, shopWangwang)
                if os.path.exists(image_outpath):
                    myFormat('店铺【%s】 已经保存过了' % shopWangwang)
                    continue
                self.driver.get(wx_shop_url)
                tag = self.driver.find_element_by_tag_name('body')
                self.driver.find_element_by_css_selector(
                    '#mp-header > section.tm-shop-header > div.shop-back-img').click()
                for num in range(40):
                    tag.send_keys(Keys.PAGE_DOWN)
                    time.sleep(0.5)
                    print('滚动次数',num)
                waitTime2(2)
                tag.send_keys(Keys.HOME)
                tag.send_keys(Keys.HOME)

                fullpage_screenshot(self.driver, image_outpath)
                myFormat('店铺【%s】处理完成' % shopWangwang)
                waitTime2(10,15)
            except Exception as e:
                print('error %s'%shopWangwang,e,)
                continue

        else:
            myFormat('所有店铺处理完成')
            self.driver.close()


    def run(self):
        self.process_many_shop()

# ╭~~~╮
# (o^.^o)
# ------- 功能------------
# 1 保存商城店铺无线首页的 全屏 截图
# 2

# ╭~~~╮
# (o^.^o)

#
# 另外建立一个错误信息收集的 小模块，类似于日志文件，看个文件出错了，出错的信息是什么，都记录到数据库中，方便查看
# 另外看看 C店如何保存的
if __name__ == '__main__':
    SF=SaveWuxianHomeFullScreenCapture()
    SF.run()


