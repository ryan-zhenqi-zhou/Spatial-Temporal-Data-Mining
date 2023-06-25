import requests
import json
import pymongo
import pandas as pd
import time
import cconv
from multiprocessing import Pool
import random
import shapefile

class RoutesDT:
    # 集中调参区1
    ############################################################
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    rd_db = myclient["玄武湖190802-111"]   # 调参1：起终点数据所处的数据库
    rd_tb_org = rd_db['湖景房小区南门坐标']     # 调参2：起点数据所处的数据表
    rd_tb_des = rd_db['房地产']     # 调参3：终点数据所处的数据表
    #district = '黄浦区'          # 调参4：筛选的区
    ############################################################
    wt_db = myclient["玄武湖190802-333"]   # 调参5：需要存储的数据库
    wt_tb = wt_db['湖景房小区南门路径']        # 调参6：需要存储的数据表
    ############################################################
    pool_num = 5        # 调参7：进程数
    rg_div = 2219       # 调参8：每个进程给的OD数量，需要运行后查看
    ############################################################
    def get_js(self, key, coord_org, coord_des):
        try:
            #######################################################################################
            # 代码段：get请求获取返回数据，json.loads转为jsond对象。。
            url = 'http://api.map.baidu.com/direction/v1?mode=walking&origin={}&destination={}&region=南昌&output=json&ak={}'. \
                format(coord_org, coord_des, key)  # 调参 这里一个地点可能改动
            print(url)
            r = requests.get(url).text
            dt = json.loads(r)
            ############################################################
            # 代码段：解析json对象中的有用数据，按字典键值对存储。。
            if dt['message'] == 'ok':
                if dt['result']['routes'][0]['duration'] <= 100000000000000:# 调参 这里一个时间可能改动

                    dt_dic = {}
                    dt_dic['distance'] = dt['result']['routes'][0]['distance']  # 各出行的距离
                    dt_dic['duration'] = dt['result']['routes'][0]['duration']  # 各出行的耗时

                    turn = 0  # 这是转弯次数
                    cgml = 0  # 这是穿过马路
                    grxd = 0  # 这是过人行道
                    gft = 0  # 这是过扶梯

                    steps_lst = []
                    steps_od_lst = []  #
                    coord_org_1 = str(coord_org.split(',')[1]) + ',' + str(coord_org.split(',')[0])
                    steps_lst.append(coord_org_1)
                    steps_od_lst.append(coord_org_1)  #
                    step_ori = str(dt['result']['routes'][0]['steps'][0]['stepOriginLocation']['lng']) + ',' + str(
                        dt['result']['routes'][0]['steps'][0]['stepOriginLocation']['lat'])
                    steps_lst.append(step_ori)
                    for m in dt['result']['routes'][0]['steps']:

                        stepInstructions_ = m['instructions']
                        if stepInstructions_.find('转') != -1:
                            turn_ = stepInstructions_.count('转')
                            turn = turn + int(turn_)
                        if stepInstructions_.find('穿过马路') != -1:
                            cgml_ = stepInstructions_.count('穿过马路')
                            cgml = cgml + int(cgml_)
                        if stepInstructions_.find('过人行道') != -1:
                            grxd_ = stepInstructions_.count('过人行道')
                            grxd = grxd + int(grxd_)
                        if stepInstructions_.find('过扶梯') != -1:
                            gft_ = stepInstructions_.count('过扶梯')
                            gft = gft + int(gft_)

                        steps_lst.append(m['path'])
                        step_des = str(m['stepDestinationLocation']['lng']) + ',' + str(
                            m['stepDestinationLocation']['lat'])
                        steps_lst.append(step_des)
                    coord_des_1 = str(coord_des.split(',')[1]) + ',' + str(coord_des.split(',')[0])
                    steps_lst.append(coord_des_1)
                    steps_od_lst.append(coord_des_1)  #
                    dt_dic['steps_lst'] = steps_lst
                    dt_dic['steps_od_lst'] = steps_od_lst  #

                    dt_dic['ori_lng_bd'] = float(coord_org.split(',')[1])
                    dt_dic['ori_lat_bd'] = float(coord_org.split(',')[0])
                    dt_dic['ori_lng_84'] = \
                        cconv.bd09_to_wgs84(float(coord_org.split(',')[1]), float(coord_org.split(',')[0]))[0]
                    dt_dic['ori_lat_84'] = \
                        cconv.bd09_to_wgs84(float(coord_org.split(',')[1]), float(coord_org.split(',')[0]))[1]
                    dt_dic['des_lng_bd'] = float(coord_des.split(',')[1])
                    dt_dic['des_lat_bd'] = float(coord_des.split(',')[0])
                    dt_dic['des_lng_84'] = \
                        cconv.bd09_to_wgs84(float(coord_des.split(',')[1]), float(coord_des.split(',')[0]))[0]
                    dt_dic['des_lat_84'] = \
                        cconv.bd09_to_wgs84(float(coord_des.split(',')[1]), float(coord_des.split(',')[0]))[1]
                    # 这里是要传入的全局变量的数据
                    ########################################################
                    dt_dic['id'] = o_id
                    dt_dic['ori_name'] = ori_name
                    dt_dic['des_name'] = des_name
                    dt_dic['address'] = address
                    dt_dic['province'] = province
                    dt_dic['city'] = city
                    dt_dic['area'] = area
                    dt_dic['h1'] = h1
                    dt_dic['h2'] = h2
                    dt_dic['uid'] = uid
                    ########################################################
                    dt_dic['step'] = len(dt['result']['routes'][0]['steps'])
                    dt_dic['turn'] = turn
                    dt_dic['cgml'] = cgml
                    dt_dic['grxd'] = grxd
                    dt_dic['gml'] = cgml + grxd
                    dt_dic['gft'] = gft

                    coords = ';'.join(steps_lst).split(';')
                    coords_od = ';'.join(steps_od_lst).split(';')  #
                    dt_dic['path_wgs84'] = [cconv.bd09_to_wgs84(float(i.split(',')[0]), float(i.split(',')[1])) for i in
                                            coords]
                    dt_dic['od_wgs84'] = [cconv.bd09_to_wgs84(float(i.split(',')[0]), float(i.split(',')[1])) for i in
                                          coords_od]  #
                    print(dt_dic)
                    self.wt_tb.insert_one(dt_dic)  # 写入数据库
                else:
                    pass
            elif dt['status'] == 302:
                print('error')
                with open('./log.txt', 'a') as fl:
                    fl.write(url + '$' + str(o_id) + '$' + str(ori_name) + '$' + str(des_name) + '$' + str(
                        address) + '$' + str(province) + '$' + str(city) + '$' + str(area) + '$' + str(h1) + '$' + str(
                        h2) + '$' + str(uid) + '\n')

                return 'NO'
            else:
                pass
        except:
            print('error')
            with open('./log.txt', 'a') as fl:
                fl.write(
                    url + '$' + str(o_id) + '$' + str(ori_name) + '$' + str(des_name) + '$' + str(address) + '$' + str(
                        province) + '$' + str(city) + '$' + str(area) + '$' + str(h1) + '$' + str(h2) + '$' + str(
                        uid) + '\n')

    def run(self,rg):
        """
        需要对起点进行分割，按10份来分
        """
        in_dt = self.rd_tb_org.find()
        dv = pd.DataFrame(in_dt)
        # dv = dv[dv['district'] == self.district]  # 如果不需要筛选，请注释掉
        dv = dv[['lat_bd', 'lng_bd', 'name']].drop_duplicates(subset=['name', 'lng_bd', 'lat_bd'])
        #dv['coords_gcj02'] = dv[['lat', 'lng']].apply(lambda x: cconv.bd09_to_gcj02(x[1], x[0]), axis=1)
        #dv['coords_bd'] = dv[['lng_bd', 'lat_bd']].apply(lambda x: '{},{}'.format(x[0], x[1]))
        dv = dv.reset_index()
        #dv = dv[['lat','lng','name']]
        ##################################################################################

        in_dt = self.rd_tb_des.find()
        ds = pd.DataFrame(in_dt)
        # ds = ds[ds['district'] == self.district]  # 如果不需要筛选，请注释掉
        ds = ds.drop_duplicates(
            subset=['name', 'lat_bd', 'lng_bd', 'lat_wgs84', 'lng_wgs84', 'address', 'uid', 'province', 'city', 'area'])
        #ds['coords_gcj02'] = ds[['lat', 'lng']].apply(lambda x: cconv.bd09_to_gcj02(x[1], x[0]), axis=1)
        #ds['coords_bd'] = ds[['lng_bd', 'lat_bd']].apply(lambda x: '{},{}'.format(x[0], x[1]))
        ds = ds.reset_index()
        #ds = ds[['lat', 'lng', 'name']]

        print('共有{}条线路'.format(len(dv) * len(ds)))
        print('需要调参rg_div改为{}'.format(int(len(dv) * len(ds) / 5) + 1))#调参，如果线程数有改变，请把这里的10替换掉
        print('现在请停止修改后再运行，该提示会出现10次，请不要惊慌')
        print('修改后再次出现上述提示请忽略')
        time.sleep(2)
        # 调参8：定义api_key的列表，目前这些key无法使用
        key_lst = ['9ppdgjXGMtAAMkgB0Y5E6SXXG0Q3uiwT',
                   '3KVc76piFpse9KKKs69AeH0Pixc22jDw',
                   'MZPKQ9Y39bdQrx4KZpCZtGjMp3WAGViC',
                   '8jrIO1gR9pdgZFznQbKkDcswiYUpzaRG',
                   '5UyXGbz2fm4rGdQrGvQgFUMhk3n1XCVD',
                   'uhSzaADTObkIvTM3eGj6h3LROfckCGEt',
                   'ZGLh9RtdjU7RQCVzrru9jEAO6mZ71Bpr',
                   'vnvPKNWpiZPpKgAen6R7x2hKxna0V2Rz',
                   'LugICWoAnuoy11EjLXTLDoeSAe6PigOF',
                   'P0IcOXZbxiTWknXFp6uSZquwAKpc3TW3',
                   'rgxAwoyfwdGxN38vbdRpEZMxpTUXNBD4']

        api_key = random.choice(key_lst)
        print(api_key)

        i = 1
        df_use = dv.iloc[rg[0]:rg[1],].reset_index()
        print(df_use)
        for m in range(len(df_use)):
            for n in range(len(ds)):
                print('该进程共有{}条线路'.format(len(df_use) * len(ds)))
                print('下载{} / {}'.format(i, len(df_use) * len(ds)))
                print('从{}到{}'.format((str(df_use['lng_bd'][m]) + ',' + str(df_use['lat_bd'][m])), (str(ds['lng_bd'][n]) + ',' + str(ds['lat_bd'][n]))))

                global o_id
                global ori_name
                global des_name
                global address
                global province
                global city
                global area
                global h1
                global h2
                global uid
                o_id = i
                ori_name = df_use['name'][m]
                des_name = ds['name'][n]
                address = ds['address'][n]
                province = ds['province'][n]
                city = ds['city'][n]
                area = ds['area'][n]
                h1 = ds['h1'][n]
                h2 = ds['h2'][n]
                uid = ds['uid'][n]
                coord_org = str(df_use['lat_bd'][m]) + ',' + str(df_use['lng_bd'][m])
                coord_des = str(ds['lat_bd'][n]) + ',' + str(ds['lng_bd'][n])

                dm = self.get_js(api_key,coord_org, coord_des)
                if dm == 'NO':
                    api_key = random.choice(key_lst)
                else:
                    pass
                i += 1

    def mt_process(self):
        p = Pool(self.pool_num)
        for i in range(self.pool_num):
            p.apply_async(self.run, args=([i * self.rg_div, self.rg_div * (i + 1)],))
            print([i * self.rg_div, self.rg_div * (i + 1)])
        print('Waiting for all subprocesses done...')
        p.close()
        p.join()

if __name__ == '__main__':
    t1 = time.time()

    ps = RoutesDT()
    ps.mt_process()

    DM = RoutesDT()
    dts = DM.wt_tb
    print('数据下载完毕，执行shp文件创建。。。')
    x = dts.find()
    x = [i for i in x]
    df = pd.DataFrame(x)

    print('数据下载完毕，执行shp文件创建。。。')
    x = dts.find()
    x = [i for i in x]
    dz = pd.DataFrame(x)
    #################################################################
    # 代码段：定义shp文件路径以及shp文件的字段名称、类型。(这个是tr路径)
    w = shapefile.Writer(r'.\shp\03{}-tr_polyline'.format(dts.name))  # 定义需要存储的shp文件路径名称
    w.field('id', 'F')
    w.field('ori_name', 'C')
    w.field('des_name', 'C')
    w.field('ori_lng_bd', 'F', decimal=20)
    w.field('ori_lat_bd', 'F', decimal=20)
    w.field('ori_lng_84', 'F', decimal=20)
    w.field('ori_lat_84', 'F', decimal=20)
    w.field('des_lng_bd', 'F', decimal=20)
    w.field('des_lat_bd', 'F', decimal=20)
    w.field('des_lng_84', 'F', decimal=20)
    w.field('des_lat_84', 'F', decimal=20)
    w.field('address', 'C')
    w.field('province', 'C')
    w.field('city', 'C')
    w.field('area', 'C')
    w.field('h1', 'C')
    w.field('h2', 'C')
    w.field('uid', 'C')
    w.field('distance', 'F')
    w.field('duration', 'F')
    w.field('step', 'F')
    w.field('turn', 'F')
    w.field('cgml', 'F')
    w.field('grxd', 'F')
    w.field('gml', 'F')
    w.field('gft', 'F')

    #################################################################
    # 代码段：遍历表格中的数据。
    dk = df.reset_index()
    for i in range(len(dk)):
        w.line([dk['path_wgs84'][i]])
        w.record(
            dk['id'][i],
            dk['ori_name'][i],
            dk['des_name'][i],
            dk['ori_lng_bd'][i],
            dk['ori_lat_bd'][i],
            dk['ori_lng_84'][i],
            dk['ori_lat_84'][i],
            dk['des_lng_bd'][i],
            dk['des_lat_bd'][i],
            dk['des_lng_84'][i],
            dk['des_lat_84'][i],
            dk['address'][i],
            dk['province'][i],
            dk['city'][i],
            dk['area'][i],
            dk['h1'][i],
            dk['h2'][i],
            dk['uid'][i],
            dk['distance'][i],
            dk['duration'][i],
            dk['step'][i],
            dk['turn'][i],
            dk['cgml'][i],
            dk['grxd'][i],
            dk['gml'][i],
            dk['gft'][i])
    w.close()
    #################################################################
    # 代码段：定义shp文件路径以及shp文件的字段名称、类型。(这个是od路径)
    wo = shapefile.Writer(r'.\shp\03{}-od_polyline'.format(dts.name))  # 定义需要存储的shp文件路径名称
    wo.field('id', 'F')
    wo.field('ori_name', 'C')
    wo.field('des_name', 'C')
    wo.field('ori_lng_bd', 'F', decimal=20)
    wo.field('ori_lat_bd', 'F', decimal=20)
    wo.field('ori_lng_84', 'F', decimal=20)
    wo.field('ori_lat_84', 'F', decimal=20)
    wo.field('des_lng_bd', 'F', decimal=20)
    wo.field('des_lat_bd', 'F', decimal=20)
    wo.field('des_lng_84', 'F', decimal=20)
    wo.field('des_lat_84', 'F', decimal=20)
    wo.field('address', 'C')
    wo.field('province', 'C')
    wo.field('city', 'C')
    wo.field('area', 'C')
    wo.field('h1', 'C')
    wo.field('h2', 'C')
    wo.field('uid', 'C')
    wo.field('distance', 'F')
    wo.field('duration', 'F')
    wo.field('step', 'F')
    wo.field('turn', 'F')
    wo.field('cgml', 'F')
    wo.field('grxd', 'F')
    wo.field('gml', 'F')
    wo.field('gft', 'F')

    #################################################################
    # 代码段：遍历表格中的数据。
    dko = df.reset_index()
    for i in range(len(dko)):
        wo.line([dko['od_wgs84'][i]])
        wo.record(
            dko['id'][i],
            dko['ori_name'][i],
            dko['des_name'][i],
            dko['ori_lng_bd'][i],
            dko['ori_lat_bd'][i],
            dko['ori_lng_84'][i],
            dko['ori_lat_84'][i],
            dko['des_lng_bd'][i],
            dko['des_lat_bd'][i],
            dko['des_lng_84'][i],
            dko['des_lat_84'][i],
            dko['address'][i],
            dko['province'][i],
            dko['city'][i],
            dko['area'][i],
            dko['h1'][i],
            dko['h2'][i],
            dko['uid'][i],
            dko['distance'][i],
            dko['duration'][i],
            dko['step'][i],
            dko['turn'][i],
            dko['cgml'][i],
            dko['grxd'][i],
            dko['gml'][i],
            dko['gft'][i])
    wo.close()
    print('shp文件全部生成。。。')

    print('all done!')
    t2 = time.time()
    print('耗时{:.2f}秒'.format(t2 - t1))