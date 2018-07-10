# coding=utf-8
# !/usr/bin/env python
import sys
import os

hadoop_cmd="/usr/local/hadoop/bin/hadoop"#hadoop安装目录
hdfs_root="/user/hadoop"
file = open('imgpath.txt','r')
local_temp_dir="/home/test_data/temp"
local_result_dir="/home/test_data/res"

if os.path.exists(local_temp_dir):
    os.system("rm -r " + local_temp_dir)
if os.path.exists(local_result_dir):
    os.system("rm -r " + local_result_dir)


os.system("mkdir "+local_temp_dir)
os.system("mkdir "+local_result_dir)
for line in file.readlines():
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split("\n")
    #读取txt中的每行的内容，然后将这个图片从hdfs上拉到子节点中，调用相应的函数处理这张图片
    #print("%s\t%d"%(words[0],1))
    #print("%s\t%d"%(words[0],len(words[0])))
    



    fs_img_path=hdfs_root+"/data/img/"+words[0]
    local_img_path=local_temp_dir+"/"+words[0]
    copy_to_local=" fs -get "+fs_img_path+" "+local_img_path
    hadoopcmd = hadoop_cmd + copy_to_local
    os.system(hadoopcmd)
    #resu_path = local_temp_dir+"/tmp.txt"
    #qpath = "/home/test_data/hh/quality " + local_img_path + " "+resu_path
    #os.system(qpath)
    GFR_path = "/home/test_data/GFR "
    fea_path = local_temp_dir+"/"+words[0].split('.')[0]+".fea"
    fea = GFR_path+"  -i "+local_img_path+"  -o "+fea_path
    os.system(fea)

    model_path = "/home/test_data/qf75.model"
    pred_path = "/home/test_data/EnsemblePredict.jar"
    resu_path = local_result_dir+"/"+words[0].split('.')[0]+".res"
    predict = "java -jar "+pred_path+" "+model_path + " "+fea_path + " "+resu_path
    os.system(predict)

    tmpfile = open(resu_path,'r')
    txt = tmpfile.readline()
    print("%s\n" %(txt))
    # print("%s\t%d"%(hadoopcmd,1))
    tmpfile.close()
file.close()
