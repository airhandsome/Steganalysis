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
    
    fs_img_path=words[0]
    img_name=words[0].split("/")[-1]
    task_id=fs_img_path.split("/")[-3]
    local_task_dir="/home/test_data/"+task_id
    hdfs_task_dir="/user/hadoop/"+task_id
    if not os.path.exists(local_task_dir):
        #subprocess.run("mkdir "+local_task_dir,shell=True)
        os.system("mkdir "+local_task_dir)
    if not os.path.exists(local_task_dir+"/temp.model"):
        model_cmd = hadoop_cmd + " fs -get "+hdfs_task_dir+"/temp.model "+local_task_dir+"/temp.model"
        os.system(model_cmd)

    local_img_path=local_task_dir+"/"+img_name
    copy_to_local=" fs -get "+fs_img_path+" "+local_img_path
    img_cmd = hadoop_cmd + copy_to_local
    os.system(img_cmd)

    GFR_path = "/home/test_data/SDA-CNN "
    fea_path = local_task_dir+"/"+img_name.split('.')[0]+".fea"
    fea = GFR_path+" -i "+local_img_path+" -o "+fea_path
    os.system(fea)
    
    model_path = local_task_dir+"/temp.model"
    pred_path = "/home/test_data/EnsemblePredict.jar"
    resu_path = local_task_dir+"/"+img_name.split('.')[0]+".res" 
    java_path = "/usr/local/jdk1.8/bin/java"
    predict = java_path+" -jar "+pred_path+" "+model_path + " "+fea_path + " "+resu_path
    os.system(predict)

    with open(resu_path,'r') as tempfile:
        txt = tempfile.readline()
        if len(txt) > 2:
            st = img_name + " " + txt
            print("%s\n" %(st))

if os.path.exists(local_task_dir):
    os.system("rm -r "+local_task_dir)

