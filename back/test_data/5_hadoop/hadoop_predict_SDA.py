# coding=utf-8
# !/usr/bin/env python
import sys
import os

hadoop_cmd="/usr/local/hadoop/bin/hadoop"#hadoop安装目录
data_root="/home/test_data/"
hdfs_root="/user/hadoop/"

def run_hadoopcmd(cmd):
    hadoopcmd = hadoop_cmd + " " + cmd
    os.system(hadoopcmd)

def get_img_path(local_img_path, local_txt_path,taskid):

    if os.path.exists(local_txt_path):
        os.system("rm -r "+local_txt_path)
    if not os.path.exists(local_txt_path):
        os.system("mkdir "+local_txt_path)

    for root, dirs, files in os.walk(local_img_path):
        for file in files:
            file_ext=os.path.splitext(file)[1]
            file_name=os.path.splitext(file)[0]
            if  file_ext== '.jpg' or file_ext=='.jpeg':
                content=hdfs_root+str(taskid)+"/img/"+file
                txt_path=local_txt_path+file_name+".txt"
                with open(txt_path, 'w') as f:
                    f.write(content)


def hadoop_51(local_img_path, local_txt_path,local_output,taskid):
    fs_task_path=hdfs_root+str(taskid)+"/"
    make_dir_path="fs -mkdir -p "+fs_task_path
    run_hadoopcmd(make_dir_path)

    copy_txt_cmd= "fs -copyFromLocal " + local_txt_path + " " + fs_task_path
    copy_img_cmd= "fs -copyFromLocal " + local_img_path + " " + fs_task_path
    run_hadoopcmd(copy_txt_cmd)
    run_hadoopcmd(copy_img_cmd)

    file_path = "/home/test_data/5_mapper_reducer/54_predict/mapper_predict.py,/home/test_data/5_mapper_reducer/54_predict/reducer_predict.py" 
    input_path = fs_task_path+"txt"
    output_path = fs_task_path + "output"

    run_hadoopcmd("jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar "\
                +"-D mapred.map.tasks=2 -D mapred.job.priority=HIGH  -D mapreduce.map.memory.mb=8192"\
		+" -files "+file_path+" -input " + input_path + " -output "+output_path \
		+" -mapper 'python mapper_predict.py' -reducer 'python reducer_predict.py'")


    get_output="fs -get " + output_path + "/part-00000 " + local_output
    run_hadoopcmd(get_output)

    delete_cmd = "fs -rm -r " + fs_task_path
    run_hadoopcmd(delete_cmd)

def main():

    taskid=4
    local_img_path = data_root+"task/"+str(taskid)+"/img/"  # 存储所有图片的文件夹
    local_txt_path = data_root+"task/"+str(taskid)+"/txt/"  # 存储所有图片名的txt文件夹

    local_output=data_root+"task/"+str(taskid)+"/result"

    get_img_path(local_img_path, local_txt_path,taskid)

    hadoop_51(local_img_path, local_txt_path,local_output,taskid)

if __name__ == '__main__':
    main()
