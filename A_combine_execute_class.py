# -*- coding: utf-8 -*-
# dengfenglai 是一个工程文件夹
from dengfenglai.lpj_class.baiduNews import baiduNews # 百度新闻模块

from dengfenglai.lpj_class.U_taobaoGetShopFans import shopFans # 店铺粉丝模块

from dengfenglai.lpj_class.U_getNewProductFromKandianbao import getNewProduct # 看店宝得到新品

from dengfenglai.lpj_class.M_juhuasuan import JuhuasuanSingle # 聚划算单品团模块

from dengfenglai.lpj_class.M_juBrand import JuBrand # 品牌团商家模块

from dengfenglai.lpj_class.saveWuxianHomeFullScreenCaptureSelf import SaveWuxianHomeFullScreenCapture # 无线首页全屏截图


from dengfenglai.config import ROOT_DIR
from dengfenglai.M_functionToolKit import waitTime ,myFormat# 通用函数模块
import datetime
from multiprocessing import Pool
from gevent import monkey
import gevent
# monkey.patch_all() # 这个和多进程  multiprocessing 不能共存 正常是这样的，要不程序一直不运行

opertor_type_dict={1:'百度新闻',2:'聚划算',3:'聚划算品牌团',4:'店铺粉丝',5:'看店宝新品',6:'无线首页全屏截图'}
is_baidu_news = 1 # 百度新闻 开关
is_juhuasuan = 1 # 聚划算单品团 开关
is_ju_brand = 1# 品牌团商家模块 开关

is_shop_fans = 1 # 店铺粉丝模块 开关
# is_new_product = 0 # 看店宝得到新品 开关,这个暂时不用了
is_wuxian_home_capture_full_screen = 0 # 无线首页全屏截图
# 这个把每个功能封装成一个类，统一调用，不用每次打开那么多文件


if __name__ == '__main__':
    # gevent.joinall([gevent.spawn(async_request, x) for x in opertor_type_dict.keys()])

    # 百度新闻模块
    if is_baidu_news:
        baidu_news=baiduNews(is_save_excel=0, is_save_database=1)
        baidu_news.main_baidu_news()


    # 聚划算单品团模块
    if is_juhuasuan:
        start=datetime.datetime.now()
        juhuasuan_single=JuhuasuanSingle(save_dir=r'{}:\聚划算'.format(ROOT_DIR), is_save_database=1,keyWord = '聚划算全部产品_',
                 temp_keyWord = '',is_save_pic = 0,totalPage=None) # 初始化，可以定义保存目录
        print('\n')
        myFormat('▶▶15秒确认以下信息◀◀')
        save_pic = '同步保存图片' if juhuasuan_single.is_save_pic else '不保存图片'
        save_database = '同步保存到数据库，请确定已经安装了mysql数据库，否非会出错' if juhuasuan_single.is_save_database else '不保存到数据库'
        myFormat('总页码是【%d】,关于图片保存，你选择的是【%s】' % (juhuasuan_single.totalPage, save_pic))
        myFormat('关于是否保存到数据库，你选择的是【%s】' % (save_database))
        waitTime(15)
        Pgroup = [x for x in range(juhuasuan_single.startPage, juhuasuan_single.totalPage + 1)]
        # Pgroup=[7,22,32,33]
        myp = Pool()
        myp.map(juhuasuan_single.main, Pgroup)
        print('2秒后开始合并数据.....')
        waitTime(2)
        print('正在合并数据.....')
        juhuasuan_single.combineEveryPageInfoToOneV2()  # 升级为 pandas 合并，效率更高
        print('合并数据完成!')
        end = datetime.datetime.now()
        print('☺☺☺☺☺☺恭喜你，全部信息保存完毕用时 %s ☺☺☺☺☺☺' % (end - start))


    # 品牌团商家模块
    if is_ju_brand:
        try:
            start = datetime.datetime.now()
            ju_brand = JuBrand(save_dir=r'{}:\聚划算'.format(ROOT_DIR),is_save_excel=1, is_save_database=1,keyWord = '品牌团_',
                     temp_keyWord = '',) # 初始化，
            allCatIDs = list(ju_brand.allCatID.keys())
            print(allCatIDs)
            # 多线程开始 同时下载全部类目数据和图片
            myp = Pool()
            myp.map(ju_brand.main, allCatIDs)
            end = datetime.datetime.now()
            print('☺☺☺☺☺☺恭喜你，全部聚划算品牌团保存完毕用时 %s ☺☺☺☺☺☺' % (end - start))
            print('------------------------>>> 保存路径是 \t %s\t <<<-----------------------------' % ju_brand.firstPath)
        except Exception as e:
            print(e,'error')
            pass

    # 店铺粉丝模块
    if is_shop_fans:
        shop_fans=shopFans(is_save_excel=0, is_save_database=1)
        shop_fans.main()


    # 看店宝得到新品
    # if is_new_product:
    #     new_product=getNewProduct(is_save_excel=1, is_save_database=1)
    #     new_product.get_many_shop_new_product()

        # 无线首页全屏截图
    if is_wuxian_home_capture_full_screen:
        SF = SaveWuxianHomeFullScreenCapture()
        SF.run()