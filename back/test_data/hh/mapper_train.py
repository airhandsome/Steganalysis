# coding=utf-8
# !/usr/bin/env python
import sys
import os

hadoop_cmd="/usr/local/hadoop/bin/hadoop"#hadoop安装目录
#hdfs_root="/user/hadoop"
local_temp_dir="/home/test_data/temp"
local_result_dir="/home/test_data/res"

if os.path.exists(local_temp_dir):
    os.system("rm -r " + local_temp_dir)
if os.path.exists(local_result_dir):
    os.system("rm -r " + local_result_dir)


os.system("mkdir "+local_temp_dir)
os.system("mkdir "+local_result_dir)


for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split("\n")
    word=words[0].split(" ")
    #读取txt中的每行的内容，然后将这个图片从hdfs上拉到子节点中，调用相应的函数处理这张图片
    #print("%s\t%d"%(words[0],1))
    #print("%s\t%d"%(words[0],len(words[0])))
    #words[0]直接存储每张图片在hdfs上的图片路径
    img_type=word[0]
    fs_img_path=word[1]
    img_name=fs_img_path.split("/")[-1]
    local_img_path=local_temp_dir+"/"+img_name
    copy_to_local=" fs -get "+fs_img_path+" "+local_img_path
    hadoopcmd = hadoop_cmd + copy_to_local
    os.system(hadoopcmd)
    GFR_path = "/home/test_data/GFR "
    fea_path = local_temp_dir+"/"+img_name.split('.')[0]+".fea"
    fea = GFR_path+"  -i "+local_img_path+"  -o "+fea_path
    os.system(fea)
    tmpfile = open(fea_path,'r')
    txt = tmpfile.readline()
    if len(txt) > 2:
        st = img_type + "," + txt;
        print("%s\n" %(st))
    # print("%s\t%d"%(hadoopcmd,1))
    tmpfile.close()
