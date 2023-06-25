import requests
import json
import pymongo
import time
import schedule   # 需要安装该库,cmd中pip install schedule
import cconv
import random
import shapefile
import pandas as pd


def get_dt(coords,api_key):
    try:
        #######################################################################################
        # 代码段：get请求获取返回数据，json.loads转为jsond对象。。
        url = 'http://restapi.amap.com/v3/traffic/status/rectangle?rectangle={}&key={}&extensions=all'. \
            format(coords,api_key)
        print(url)
        r = requests.get(url).text
        sjson = json.loads(r)

        ############################################################
        # 代码段：解析json对象中的有用数据，按字典键值对存储。。
        if sjson['info'] == "OK":
            if 'trafficinfo' in sjson:
                if len(sjson['trafficinfo']['roads']) != 0:
                    rds = sjson['trafficinfo']['roads']
                    for rd in rds:
                        dt = {}
                        keys = ['name','status','direction','angle','speed','lcodes','polyline']
                        for key in keys:
                            if key in rd:
                                dt[key] = rd[key]
                            else:
                                dt[key] = None
                        if 'polyline' in rd:
                            dt['line_wgs84'] = [cconv.gcj02_to_wgs84(float(x.split(',')[0]), float(x.split(',')[1])) for x in dt['polyline'].split(';')]
                        else:
                            dt['line_wgs84'] = None
                        dt['record_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(dt)
                        table_wt.insert_one(dt)
                else:
                    print('{}--->no data 1'.format(coords))
            else:
                print('{}--->no data 2'.format(coords))

        elif sjson['info'] == 'DAILY_QUERY_OVER_LIMIT':
            print('error')
            with open('./log.txt', 'a') as fl:
                fl.write(url + '\n')
            return 'NO'
        else:
            pass
    except:
        print('error')
        with open('./log.txt', 'a') as fl:
            fl.write(url + '\n')

def run():
    t1 = time.time()
    print('I am running')

    #################################################################################
    # 代码段：定义表，每执行一次新创建一张表
    tb_name = time.strftime("%Y-%m-%d-%H-%M", time.localtime())  # 按年月日小时分钟作为表的名称
    global table_wt
    table_wt = db[tb_name]

    #################################################################################
    # 代码段：随机提取一个api_key
    api_key = random.choice(key_lst)
    print(api_key)

    #################################################################################
    # 代码段：读取网格坐标串
    data = table_rd.find()
    for dt in data:
        print(dt['coords'])
        dm = get_dt(dt['coords'],api_key)

        ################################################################################
        # 代码段：判断配额是否用完，用完切换api_key
        if dm == 'NO':
            api_key = random.choice(key_lst)
        else:
            pass
    t2 = time.time()
    print('done')
    print('耗时{:.2f}'.format(t2-t1))

    ################################################################################
    # 代码段：读取下载系列的交通态势数据
    print('{}数据下载完毕，执行shp文件创建。。。'.format(tb_name))
    data_to_shp = table_wt.find()
    x = [i for i in data_to_shp]
    df = pd.DataFrame(x)

    #################################################################
    # 代码段：定义shp文件路径以及shp文件的字段名称、类型。
    w = shapefile.Writer(r'.\shp\04traffic{}lines'.format(tb_name))
    w.field('name', 'C')  #
    w.field('status', 'F')
    w.field('direction', 'C')
    w.field('angle', 'F')  #
    w.field('speed', 'F')

    #################################################################
    # 代码段：遍历表格中的数据。
    df = df.drop_duplicates(subset='direction')
    dv = df.reset_index()
    for i in range(len(dv)):
        w.line([dv['line_wgs84'][i]])
        w.record(
            dv['name'][i],
            dv['status'][i],
            dv['direction'][i],
            dv['angle'][i],
            dv['speed'][i],
        )
    w.close()


if __name__ == "__main__":
    print('Started...')

    # 集中调参区域
    ################################################################################
    key_lst = ['d2feb54202eda3a5ac9ba74624138ad4',   # 调参1：定义api_key的列表，目前这些key无法使用
               '6ee02bcc64ba94e4e83d5d0f8fced742',
               'f0ff3fb241a80e1c26132b86b3e7d4ac',
               'abd5b3ce9d2e61cdcd7148a0d07758af',
               '03a9132711fa8d023136759144f57895',
               'd6d2e585646cfa8c08723aa0e206b5f0',
               'e8c7d0d83027ce32d58d5489f8c6b8cc',
               '832a9bbdafc3c974ce987823e18d3486',
               'ef58d67a57b0c3553584d93f88aa76f4']

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    db = myclient["深圳路况20201214"]             # 调参2：定义需要读取的数据库
    table_rd = db['coords_list']            # 调参3：使用L4_TRAFFIC数据库的表，也就是筛选后的坐标。
    table_wt = db["data_all"]               # 调参4：定义需要写入的数据表，也就是把爬取的数据存放到哪一张表里。


    #run()
    #schedule.every(3).minutes.do(run)       # 调参5：每几分钟爬取一次？这里为每5分钟。

    ######### 以下为爬取每天早高峰时刻，间隔15分钟的数据，
    ######### 其他的自定义时间见https://pypi.org/project/schedule
    schedule.every().day.at("07:00").do(run)
    schedule.every().day.at("07:15").do(run)
    schedule.every().day.at("07:30").do(run)
    schedule.every().day.at("07:45").do(run)
    schedule.every().day.at("08:00").do(run)
    schedule.every().day.at("16:30").do(run)
    schedule.every().day.at("16:45").do(run)
    schedule.every().day.at("17:00").do(run)
    schedule.every().day.at("17:15").do(run)
    schedule.every().day.at("17:30").do(run)
    #schedule.every().day.at("16:00").do(run)
    #schedule.every().day.at("17:00").do(run)
    #schedule.every().day.at("18:00").do(run)
    #schedule.every().day.at("19:00").do(run)
    #schedule.every().day.at("20:00").do(run)
    #schedule.every().day.at("21:00").do(run)
    #schedule.every().day.at("22:00").do(run)
    #schedule.every().day.at("23:00").do(run)


    while True:
        schedule.run_pending()
        time.sleep(1)





