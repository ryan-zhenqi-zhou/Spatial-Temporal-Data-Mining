import requests
import json
import time
import pymongo
import cconv
from multiprocessing import Pool
import shapefile
import pandas as pd
import random

class DivdCorrds:
    """
    网格划分的类，

    """

    # 集中调参区域1
    ########################################################
    loc_all = '119.934026,30.232018, 120.087816,30.348367'  # 调参1：输入的矩形区域坐标串
    div = 0.0025                                            # 调参2：网格区间
    rg = 583                                                  # 调参3：每个进程分配的网格,通过查看小网格数量确定
    ########################################################

    def lng_lat(self):
        lng_sw = float(self.loc_all.split(',')[0])
        lng_ne = float(self.loc_all.split(',')[2])
        lat_sw = float(self.loc_all.split(',')[1])
        lat_ne = float(self.loc_all.split(',')[3])

        lng_list = [str(lng_ne)]
        while lng_ne - lng_sw >= 0:
            m = lng_ne - self.div
            lng_ne = lng_ne - self.div
            lng_list.append('%.2f' % m)

        lat_list = [str(lat_ne)]
        while lat_ne - lat_sw >= 0:
            m = lat_ne - self.div
            lat_ne = lat_ne - self.div
            lat_list.append('%.2f' % m)
        return [sorted(lng_list), lat_list]

    def get_coords(self):
        lng = self.lng_lat()[0]
        lat = self.lng_lat()[1]

        dt = ['{},{}'.format(lat[i], lng[i2]) for i in range(0, len(lat)) for i2 in range(0, len(lng))]
        lst = []
        for i in range(len(lat)):
            lst.append(dt[i * len(lng):(i + 1) * len(lng)])

        ls = []
        for n in range(0, len(lat) - 1):
            for i in range(0, len(lng) - 1):
                ls.append([lst[n + 1][i], lst[n][i + 1]])
        return ls

    def div_ret(self):
        coord_divs = self.get_coords()
        print(len(coord_divs))
        print('需要调参rg改为{}'.format(int(len(coord_divs) / 5) + 1)) #调参，如果线程数有改变，请把这里的10替换掉
        print('现在请停止修改后再运行，该提示会出现{}次，请不要惊慌！'.format(DownloadDT().num_processing))
        print('修改后再次出现上述提示请忽略')
        time.sleep(2)
        return [coord_divs[i:i + self.rg] for i in range(0, len(coord_divs), self.rg)]


class DownloadDT:
    """
    执行数据的下载
    """
    # 集中调参区域2
    ########################################################
    num_processing = 5                   # 调参4：执行多少进程，
    # 调参5：输入的POI类型，分一级、二级行业分类
    pois = {
        '美食': ['中餐厅', '外国餐厅','小吃快餐店','蛋糕甜品店','咖啡厅','茶座','酒吧'],
        '酒店': ['星级酒店','快捷酒店','公寓式酒店','民宿'],
        '购物': ['购物中心','百货商场','超市','便利店','家具建材','家电数码','商铺','集市'],
        '生活服务': ['通讯营业厅','邮局','物流公司','售票处','洗衣店','图文快印店','照相馆','房产中介机构','公用事业','维修点','家政服务','殡葬服务','彩票销售点','宠物服务','报刊亭','公共厕所','步骑行专用道驿站'],
        '丽人': ['美容','美发','美甲','美体'],
        '旅游景点': ['公园','动物园','植物园','游乐园','博物馆','水族馆','海滨浴场','文物古迹','教堂','风景区','景点','寺庙'],
        '休闲娱乐': ['度假村','农家院','电影院','ktv','剧院','歌舞厅','网吧','游戏场所','洗浴按摩','休闲广场'],
        '运动健身': ['体育场馆','极限运动场所','健身中心'],
        '教育培训': ['高等院校','中学','小学','幼儿园','成人教育','亲子教育','特殊教育学校','留学中介机构','科研机构','培训机构','图书馆','科技馆'],
        '文化传媒': ['新闻出版','广播电视','艺术团体','美术馆','展览馆','文化宫'],
        '医疗':['综合医院','专科医院','诊所','药店','体检机构','疗养院','急救中心','疾控中心','医疗器械','医疗保健'],
        '汽车服务': ['汽车销售','汽车维修','汽车美容','汽车配件','汽车租赁','汽车检测场'],
        '交通设施': ['飞机场','火车站','地铁站','地铁线路','长途汽车站','公交车站','公交线路','港口','停车场','加油加气站','服务区','收费站','桥','充电站','路侧停车位','普通停车位','接送点'],
        '金融': ['银行','ATM','信用社','投资理财','典当行'],
        '房地产': ['写字楼','住宅区','宿舍','内部楼栋'],
        '公司企业': ['公司','园区','农林园艺','厂矿'],
        '政府机构': ['中央机构','各级政府','行政单位','公检法机构','涉外机构','党派团体','福利机构','政治教育机构','社会团体','民主党派','居民委员会'],
        '出入口': ['高速公路出口','高速公路入口','机场出口','机场入口','车站出口','车站入口','门','停车场出入口','自行车高速出口','自行车高速入口','自行车高速出入口'],
        '自然地物': ['岛屿','山峰','水系'],
        '行政地标': ['省','省级城市','地级市','区县','商圈','乡镇','村庄'],
        '门址': ['门址点']}
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    db = myclient["未来科技城190813"]             #：调参6：定义存入的数据库名称
    # 调参7：定义api_key的列表
    key_lst = ['9ppdgjXGMtAAMkgB0Y5E6SXXG0Q3uiwT',
               '3KVc76piFpse9KKKs69AeH0Pixc22jDw',
               '5UyXGbz2fm4rGdQrGvQgFUMhk3n1XCVD',
               'uhSzaADTObkIvTM3eGj6h3LROfckCGEt',
               'ZGLh9RtdjU7RQCVzrru9jEAO6mZ71Bpr',
               'vnvPKNWpiZPpKgAen6R7x2hKxna0V2Rz',
               'P0IcOXZbxiTWknXFp6uSZquwAKpc3TW3',
               'MZPKQ9Y39bdQrx4KZpCZtGjMp3WAGViC']
    cityC = '杭州市' # 调参8：更改你要爬取的城市名称，需要填入“市”
    ########################################################

    def get_data(self, query, loc,api_key):
        urls = []
        for i in range(0, 20):
            url = 'https://api.map.baidu.com/place/v2/search?query=' + query +\
                '&bounds=' + loc + '&page_size=20&page_num=' + str(i) +\
                  '&output=json&ak=' + api_key       # 调参：更换key
            urls.append(url)
        ########################################################
        for i, url in enumerate(urls):  # enumerate
            try:
                print(i, url)
                js = requests.get(url).text
                data = json.loads(js)
                if 'results' in data:
                    if data['total'] != 0:
                        for item in data['results']:
                            if 'address' in item:
                                js = {}
                                js['name'] = item['name']
                                js['lat_bd'] = item['location']['lat']
                                js['lng_bd'] = item['location']['lng']
                                js['address'] = item['address']
                                js['uid'] = item['uid']
                                js['province'] = item['province']
                                js['city'] = item['city']
                                js['area'] = item['area']

                                js['h1'] = h1
                                js['h2'] = h2

                                ###############################################
                                # 代码段：使用自定义的cconv库执行坐标转换。。
                                js['lat_wgs84'] = cconv.bd09_to_wgs84(js['lng_bd'], js['lat_bd'])[1]
                                js['lng_wgs84'] = cconv.bd09_to_wgs84(js['lng_bd'], js['lat_bd'])[0]

                                if '{}'.format(self.cityC) == js['city']:
                                    print(js)
                                    tb.insert_one(js)
                                else:
                                    print('非请求市数据，跳过...')
                            else:
                                js = {}
                                js['name'] = item['name']
                                js['lat_bd'] = item['location']['lat']
                                js['lng_bd'] = item['location']['lng']
                                js['address'] = '无'
                                js['uid'] = item['uid']
                                js['province'] = item['province']
                                js['city'] = item['city']
                                js['area'] = item['area']

                                js['h1'] = h1
                                js['h2'] = h2

                                ###############################################
                                # 代码段：使用自定义的cconv库执行坐标转换。。
                                js['lat_wgs84'] = cconv.bd09_to_wgs84(js['lng_bd'], js['lat_bd'])[1]
                                js['lng_wgs84'] = cconv.bd09_to_wgs84(js['lng_bd'], js['lat_bd'])[0]

                                if '{}'.format(self.cityC) == js['city']:
                                    print(js)
                                    tb.insert_one(js)
                                else:
                                    print('非请求市数据，跳过...')

                    else:
                        print('本页及以后无数据')
                        break
                elif data['status'] == 302:
                    print('需要更换AK!!!')
                    print('error')
                    with open('./log.txt', 'a') as fl:
                        fl.write(url + '\n')
                    return 'NO'
                elif data['status'] == 401:
                    print('需要更换AK!!!')
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

        ########################################################
    def run(self,i):
        #################################################################################
        # 代码段：随机提取一个api_key
        api_key = random.choice(self.key_lst)
        print(api_key)
        time.sleep(5)

        coords = DivdCorrds()
        locs = coords.div_ret()
        # print(locs)
        ################################################################################
        # 代码段：遍历行业分类，执行函数，获取数据以及shp文件创建
        global h1
        for h1, v in self.pois.items():
            print('爬取：', h1)

            global tb
            tb = self.db[h1]
            for loc_to_use in locs[i]:
                cds = '{},{}'.format(loc_to_use[0], loc_to_use[1])
                global h2
                for h2 in v:
                    dm = self.get_data(h2, cds,api_key)
                    ################################################################################
                    # 代码段：判断配额是否用完，用完切换api_key
                    if dm == 'NO':
                        api_key = random.choice(self.key_lst)
                    else:
                        pass

    def mt_process(self):
        p = Pool(self.num_processing)
        for m in range(self.num_processing):
            p.apply_async(self.run,args=(m,))
        p.close()
        p.join()

if __name__ == "__main__":
    print("开始爬数据，请稍等...")
    start_time = time.time()

    # step1:请先把下面两行代码运行，计算出网格数量，然后根据进程数量划分。
    nums = DivdCorrds()
    nums.div_ret()
    # time.sleep(10)

    # step2:调整结束后，把前面两行代码注释掉后运行下面代码。
    pr = DownloadDT()
    pr.mt_process()

    print('数据下载完毕，执行shp文件创建。。。')
    DM = DownloadDT()
    pois = DM.pois
    db = DM.db
    for tp in pois.keys():
        print('{}数据下载完毕，执行shp文件创建。。。'.format(tp))
        x = db[tp].find()
        x = [i for i in x]

        df = pd.DataFrame(x)
        df = df.drop_duplicates(subset=['name', 'lng_bd', 'lat_bd', 'lng_wgs84', 'lat_wgs84', 'address', 'province', 'city', 'area', 'uid'])

        w = shapefile.Writer(r'.\shp\01{}poi'.format(tp))
        w.field('name', 'C')   # C代表文本类型的字段
        w.field('lng_bd','F',decimal=20)
        w.field('lat_bd', 'F', decimal=20)
        w.field('lng_wgs84', 'F', decimal=20)
        w.field('lat_wgs84', 'F', decimal=20)
        w.field('address', 'C')
        w.field('province','C')
        w.field('city', 'C')
        w.field('area', 'C')
        w.field('uid', 'C')
        w.field('h1', 'C')
        w.field('h2', 'C')

        dv = df.reset_index()
        for i in range(len(dv)):
            w.point(dv['lng_wgs84'][i],dv['lat_wgs84'][i])
            w.record(
                dv['name'][i],
                dv['lng_bd'][i],
                dv['lat_bd'][i],
                dv['lng_wgs84'][i],
                dv['lat_wgs84'][i],
                dv['address'][i],
                dv['province'][i],
                dv['city'][i],
                dv['area'][i],
                dv['uid'][i],
                dv['h1'][i],
                dv['h2'][i])
    print('shp文件全部生成。。。')

    end_time = time.time()
    print("数据爬取完毕，用时%.2f秒" % (end_time - start_time))