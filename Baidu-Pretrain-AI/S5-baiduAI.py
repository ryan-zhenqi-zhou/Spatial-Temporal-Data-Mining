# -*- coding: utf-8 -*-
from urllib import request
import ssl
import json
import base64
import requests
import os


def write_txt(path,content):
    f = open(path,'a',encoding='utf-8')
    f.write(content)
    f.close()

def judgement(a,b,c):
    if a in b :
        return c
    else:
        return 0

if __name__ == '__main__':
    i = 1
    client_id = "Ip9ndjPhhZdaHkWBceYilcLV"  #调参 官网获取的AK
    client_secret = "BM4ZNG36z1CP8oOFGeBIoSrBKexvzO8C"  #调参 官网获取的SK
    image_path = "C:\\test\\"  #调参 图片存放地址
    txt_path = "C:\\cg21.txt"  #调参 文本存放地址


    write_txt(txt_path,"照片名称,山,水,田,湖泊,草,竹,植物,花,道路,建筑,人,动物,车,指示牌,栏杆,座椅,石头,雕像"+"\n")
    gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    host = "https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=" + client_id + "&client_secret=" + client_secret
    req = request.Request(host)
    response = request.urlopen(req, context=gcontext).read().decode('UTF-8')
    result = json.loads(response)
    if (result):
        print(result)
        print(result['access_token'])

    file_names = os.listdir(image_path)
    for file_name in file_names:
        f = open(image_path+file_name, 'rb')
        img = base64.b64encode(f.read())
        host = 'https://aip.baidubce.com/rest/2.0/image-classify/v2/advanced_general'
        headers={
           'Content-Type':'application/x-www-form-urlencoded'
        }
        access_token= result['access_token']
        host=host+'?access_token='+access_token
        data={}
        data['access_token']=access_token
        data['image'] =img
        res = requests.post(url=host,headers=headers,data=data)
        req=res.json()
        if(req['result']):
            lists = req['result']
            print(lists)
            total_score_shan = total_score_shui = total_score_tian =total_score_hu =total_score_cao =total_score_zhu =total_score_zhiwu =total_score_hua =total_score_daolu = 0
            total_score_jianzhu = total_score_ren = total_score_dongwu = total_score_che = total_score_zhishipai = total_score_langang = total_score_zuoyi = total_score_shitou = total_score_diaoxiang = 0
            for list in lists:
                #1、山
                score_shan = judgement("山",list['keyword'],list['score'])
                total_score_shan = total_score_shan + score_shan
                #2、水
                score_shui = judgement("水",list['keyword'],list['score'])
                score_xi = judgement("溪", list['keyword'], list['score'])
                total_score_shui = total_score_shui + score_shui + score_xi
                #3、田
                score_tian = judgement("田",list['keyword'],list['score'])
                total_score_tian = total_score_tian + score_tian
                #4、湖泊
                score_hu = judgement("湖",list['keyword'],list['score'])
                score_shuiku = judgement("水库", list['keyword'], list['score'])
                total_score_hu = total_score_hu + score_hu + score_shuiku
                #5、草
                score_cao = judgement("草",list['keyword'],list['score'])
                total_score_cao = total_score_cao + score_cao
                #6、竹
                score_zhu = judgement("竹",list['keyword'],list['score'])
                total_score_zhu = total_score_zhu + score_zhu
                #7、植物
                score_lin = judgement("林", list['keyword'],list['score'])
                score_zhiwu= judgement("植物", list['keyword'], list['score'])
                score_shu = judgement("树", list['keyword'],list['score'])
                total_score_zhiwu = total_score_zhiwu + score_lin + score_zhiwu + score_shu
                #8、花
                score_hua= judgement("花", list['keyword'], list['score'])
                total_score_hua = total_score_hua + score_hua
                #9、道路
                score_lu = judgement("路", list['keyword'], list['score'])
                score_dao = judgement("道", list['keyword'], list['score'])
                total_score_daolu = total_score_daolu + score_lu + score_dao
                #10、建筑
                score_jianzhu = judgement("建筑", list['keyword'], list['score'])
                score_juminglou= judgement("居民楼", list['keyword'], list['score'])
                total_score_jianzhu = total_score_jianzhu + score_jianzhu + score_juminglou
                #11、人
                score_ren = judgement("人", list['keyword'], list['score'])
                score_hezhao = judgement("合照", list['keyword'], list['score'])
                total_score_ren = total_score_ren + score_ren + score_hezhao
                #12、动物
                score_niao = judgement("鸟", list['keyword'], list['score'])
                score_dongwu = judgement("动物", list['keyword'], list['score'])
                total_score_dongwu = total_score_dongwu + score_niao + score_dongwu
                #13、车
                score_che = judgement("车", list['keyword'], list['score'])
                total_score_che = total_score_che + score_che
                #14、指示牌
                score_zhishipai = judgement("指示牌", list['keyword'], list['score'])
                score_lubiao= judgement("路标", list['keyword'], list['score'])
                total_score_zhishipai = total_score_zhishipai + score_zhishipai + score_lubiao
                #15、栏杆
                score_langang = judgement("栏杆", list['keyword'], list['score'])
                total_score_langang = total_score_langang + score_langang
                #16、座椅
                score_yizi = judgement("椅", list['keyword'], list['score'])
                score_deng = judgement("凳", list['keyword'], list['score'])
                total_score_zuoyi = total_score_zuoyi + score_yizi + score_deng
                #17、石头
                score_shi = judgement("石", list['keyword'], list['score'])
                score_yan = judgement("岩", list['keyword'], list['score'])
                total_score_shitou =  total_score_shitou + score_shi + score_yan
                #18、雕像
                score_diaoxiang = judgement("雕像", list['keyword'], list['score'])
                total_score_diaoxiang = total_score_diaoxiang  + score_diaoxiang
            content_cg = file_name.split(".")[0] + ","+str(total_score_shan)+ ","+str(total_score_shui)+ ","+str(total_score_tian)+ ","+str(total_score_hu)+ ","+str(total_score_cao)+ ","+str(total_score_zhu)+ ","+str(total_score_zhiwu)+ ","+str(total_score_hua)+ ","+str(total_score_daolu)+ ","+str(total_score_jianzhu)+ ","+str(total_score_ren)+ ","+str(total_score_dongwu)+ ","+str(total_score_che)+ ","+str(total_score_zhishipai)+ ","+str(total_score_langang)+ ","+str(total_score_zuoyi)+ ","+str(total_score_shitou)+ ","+str(total_score_diaoxiang)+"\n"
            print(content_cg)
            write_txt(txt_path,content_cg)
            print("第"+str(i)+"张识别完成！")
            i = i+1
        else:
            print("超出识别标签范围，无法识别！")