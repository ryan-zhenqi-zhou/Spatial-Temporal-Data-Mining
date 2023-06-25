import requests
import json
import pandas as pd
import pymongo
import time
import cconv
import shapefile

if __name__ == "__main__":

    # 集中调参区域1
    #######################################################################################
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    write_db = myclient["苏州小区210129_1"]   # 调参4：定义需要写入的数据库
    dts = write_db['房地产围栏']           # 调参5：定义write_db数据库中需要写入的数据库
    #######################################################################################

    #######################################################################################
    # 代码段：循环dm表，提取每一行记录中的数据，并调用get_coords函数执行数据下载任务。

    #######################################################################################
    # 代码段：读取下载好的数据并转为pandas表对象，便于后面进行shp文件创建
    print('数据下载完毕，执行shp文件创建。。。')
    x = dts.find()
    x = [i for i in x]
    df = pd.DataFrame(x)

    #################################################################
    # 代码段：定义shp文件路径以及shp文件的字段名称、类型。
    w = shapefile.Writer(r'.\shp\02{}poly'.format(dts.name))   # 定义需要存储的shp文件路径名称
    w.field('name', 'C')  #
    w.field('lng_bd', 'F', decimal = 20)  # F为浮点数，decimal=6为保留小数点后6位
    w.field('lat_bd', 'F', decimal = 20)
    w.field('lng_wgs84', 'F', decimal=20)
    w.field('lat_wgs84', 'F', decimal=20)
    w.field('address', 'C')
    w.field('uid', 'C')
    w.field('province', 'C')
    w.field('city', 'C')
    w.field('area', 'C')
    w.field('h1', 'C')
    w.field('h2', 'C')



    #################################################################
    # 代码段：遍历表格中的数据。
    dv = df.reset_index()
    for i in range(len(dv)):
        w.poly([dv['coords_wgs84'][i]])  # 创建线
        w.record(                        # 创建字段
                 dv['name'][i],#.encode('gbk')
                 dv['lng_bd'][i],
                 dv['lat_bd'][i],
                 dv['lng_wgs84'][i],
                 dv['lat_wgs84'][i],
                 dv['address'][i],
                 dv['uid'][i],
                 dv['province'][i],
                 dv['city'][i],
                 dv['district'][i],
                 dv['h1'][i],
                 dv['h2'][i])
    w.close()
    print('shp文件全部生成。。。')

    print('ALL DONE!')
