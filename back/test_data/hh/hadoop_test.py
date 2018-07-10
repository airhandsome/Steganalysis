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

def getimgpath(local_img_path, local_txt_path):
    #读取文件夹下的所有图片名称，并且将名称对应的路径写到对应的txt文件中
    if not os.path.exists(local_txt_path):
        os.system("mkdir "+local_txt_path)
    else:
        os.system("rm "+local_txt_path+"/*")
    for root, dirs, files in os.walk(local_img_path):
        for file in files:
            file_ext=os.path.splitext(file)[1]
            file_name=os.path.splitext(file)[0]
            if  file_ext== '.jpg' or file_ext=='.jpeg':
                L=[]
                L.append(file)
                txt_path=local_txt_path+"/"+file_name+".txt"
                with open(txt_path, 'w') as f:
                    f.writelines(L)
def main():
    taskid=1
    local_img_path = data_root+str(taskid)+"/img"  # 存储所有图片的文件夹
    local_txt_path = data_root+str(taskid)+"/txt"  # 存储所有图片名的txt文件夹

    #getimgpath(local_img_path, local_txt_path)
    #写入到txt文件后，将图像文件和txt文件上传到hdfs上
    fs_task_path=hdfs_root+str(taskid)
    make_dir_path="fs -mkdir -p "+fs_task_path
    run_hadoopcmd(make_dir_path)
    copy_txt_cmd= "fs -copyFromLocal " + local_txt_path + " " + fs_task_path
    copy_img_cmd= "fs -copyFromLocal " + local_img_path + " " + fs_task_path
    run_hadoopcmd(copy_txt_cmd)
    run_hadoopcmd(copy_img_cmd)
    file_path = "/home/test_data/hh/mapper.py,/home/test_data/hh/reducer.py" 
    input_path = fs_task_path+"/txt"
    output_path = hdfs_root + str(taskid) + "/output"
    delete_cmd = "fs -rm -r " + output_path
    run_hadoopcmd(delete_cmd)
    run_hadoopcmd("jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar "\
                +"-D mapred.map.tasks=2 -D mapred.job.priority=HIGH  -D mapreduce.map.memory.mb=8192"\
		+" -files "+file_path+" -input " + input_path + " -output "+output_path \
		+" -mapper 'python mapper.py' -reducer 'python reducer.py'")

'''
${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar -D mapred.job.name="streaming_img" -D mapred.map.tasks=5 -D mapred.reduce.tasks=5 -D mapred.job.priority=HIGH  -D mapreduce.map.memory.mb=8192 -files "/home/test_data/hh/mapper.py,/home/test_data/hh/reducer.py"  -input /user/hadoop/data/txt -output /user/hadoop/data/output -mapper "python mapper.py" -reducer "python reducer.py"

'''
'''
    in_path = fs_txt_path+"/imgpath.txt"
    out_path = fs_txt_path+"/output"
    mapper_path="/home/test_data/hh/mapper.py"
    reducer_path="/home/test_data/hh/reducer.py"
    #将txt文件上传完后，调用mapper和reducer处理stdin的内容，也就是txt中的内容
    run_hadoopcmd("jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar " \
              + "-input " + in_path + " -output " + out_path \
              + " -mapper " + mapper_path + " -reducer " + reducer_path \
              + " -file " + mapper_path + " -file " + reducer_path \
              )
'''

if __name__ == '__main__':
    main()
