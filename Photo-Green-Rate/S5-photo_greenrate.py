# -*- coding: utf-8 -*-
import cv2
import os
import time


image_path ="E:\\test\\"   #调参  原始图片保存位置
Txt_path = "E:\\result.txt"    #调参  成果文本保存位置
save_path_hsv = "E:\\hsv"   #调参  成果图片保存位置


def brg2hsv(image_path,save_path_hsv):
    f = open(Txt_path, 'a')
    f.write("Name,Percentage"+"\n")
    f.close()
    filenames = os.listdir(image_path)
    for filename in filenames:
        t = int(time.time())
        examname = filename[:-4]
        type = filename.split('.')[-1]
        img = cv2.imread(image_path + filename)
        img_hsv = cv2.cvtColor(img,cv2.COLOR_BGR2HSV)
        save_hsv = save_path_hsv +"\\"+ examname + '_HSV'+'.'+type
        cv2.imwrite(save_hsv,img_hsv)
        row_num = img_hsv.shape[0]
        column_num = img_hsv.shape[1]
        k=0
        for i in range(0,row_num):
            for j in range(0,column_num):
                if 77> img_hsv[i,j,0] >35:
                    k=k+1
        Percentage=k/(row_num*column_num)
        result=filename.split(".")[0]+".jpg"+","+str(Percentage)+"\n"
        f = open(Txt_path, 'a')
        f.write(result)
        f.close()
        print(filename+" "+"识别完成！"+"用时："+str(int(time.time()-t))+"s")

if __name__ == '__main__':
    brg2hsv(image_path, save_path_hsv)

