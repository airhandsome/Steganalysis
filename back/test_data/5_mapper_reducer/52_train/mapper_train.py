# coding=utf-8
# !/usr/bin/env python
import sys
import os
import time

hadoop_cmd="/usr/local/hadoop/bin/hadoop"#hadoop安装目录

local_task_dir=""


for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split("\n")
    img_type=words[0].split(" ")[0]
    fs_img_path=words[0].split(" ")[1]
    #words[0].split(" ")[0]保存的图像的类型,分别对应cover和stego图像
    img_name=fs_img_path.split("/")[-1]
    task_id=fs_img_path.split("/")[-3]
    local_task_dir="/home/test_data/"+task_id
    if not os.path.exists(local_task_dir):
        os.system("mkdir "+local_task_dir)
    local_img_path=local_task_dir+"/"+img_name
    copy_to_local=" fs -get "+fs_img_path+" "+local_img_path
    hadoopcmd = hadoop_cmd + copy_to_local
    os.system(hadoopcmd)
    GFR_path = "/home/test_data/SDA-CNN "
    fea_path = local_task_dir+"/"+img_name.split('.')[0]+".fea"
    fea = GFR_path+" -i "+local_img_path+" -o "+fea_path
    os.system(fea)
    with open(fea_path,'r') as tempfile:
        txt = tempfile.readline()
        if len(txt) > 2:
            st = img_type + "," + txt
            print("%s\n" %(st))
if os.path.exists(local_task_dir):
    os.system("rm -r "+local_task_dir)
