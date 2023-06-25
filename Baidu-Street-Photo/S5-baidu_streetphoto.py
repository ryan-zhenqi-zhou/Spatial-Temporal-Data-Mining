# -*- coding:utf-8 -*-

import urllib.request
import time
import requests
import json
import pandas as pd
import time
import random

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}

def pic_download(pic_path,f_pic):
    web = urllib.request.urlopen(pic_path)
    itdata = web.read()
    f_tup = open(f_pic, "wb")
    f_tup.write(itdata)
    f_tup.close()

def read_line(f):
    temp_list=[]
    with open(f, encoding='utf-8', mode='r')as file_object:
        for line in file_object:
            temp = line.replace('\n', '').encode('utf-8').decode('utf-8-sig')
            temp_list.append(temp)
    return temp_list

def write_txt(path,content):
    f = open(path,'a',encoding='utf-8')
    f.write(content)
    f.close()

def get_data(key_o,lng,lat,i,j):
    url = "http://api.map.baidu.com/panorama/v2?ak=" + key_o + "&width=1024&height=512&location=" + lng + "," + lat + "&fov=90&heading=" + str(i * 90)
    try:
        print(url)
        js = requests.get(url).text
        if js.find('status') == -1:
            time.sleep(3)
            pic_download(url, f_pic + "\\" + j + "_" + str(i) + '.jpg')
            content = j + ',' + lng + ',' + lat + ',' + j + '_' + str(
                i) + '.jpg' + "\n"
            write_txt(f_path, content)
            print("下载完成第" + str(i + 1) + "张！")
        else:
            print('需要更换AK!!!')
            with open('./log.txt', 'a') as fl:
                fl.write(url + str(i) + str(j) +'\n')
            return 'NO'
    except:
        print('error')
        with open('./log.txt', 'a') as fl:
            fl.write(url + str(i) + str(j) + '\n')

def main(lists):
    key = random.choice(key_lst)
    print(key)
    time.sleep(5)
    for list in lists:
        temp_result = list.replace(" ","").split(",")
        print(temp_result[0]+" "+temp_result[1]+" "+temp_result[2])
        for i in range(0,4):
            dm = get_data(key, temp_result[1], temp_result[2], i,temp_result[0])
            if dm == 'NO':
                key = random.choice(key_lst)
            else:
                pass

if __name__ == '__main__':
    f = "F:\\01桌面链接\\公园可达性课题\\02玄武湖可达性分析\\12最终图像抓取修正\\02图像抓取最终汇总\\test补\\Trans_xy.txt" #调参，经纬度坐标文件
    f_pic = "F:\\01桌面链接\\公园可达性课题\\02玄武湖可达性分析\\12最终图像抓取修正\\02图像抓取最终汇总\\test补\\test2\\" # 调参，图片存放地址
    f_path = "F:\\01桌面链接\\公园可达性课题\\02玄武湖可达性分析\\12最终图像抓取修正\\02图像抓取最终汇总\\test补\\message.txt" # 调参，存放成果文本

    key_lst = [] #调参

    write_txt(f_path,"Id,lon,lat,pic_name"+"\n")
    lists = read_line(f)

    main(lists)
