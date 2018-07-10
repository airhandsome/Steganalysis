# coding=utf-8
# !/usr/bin/env python
import sys
import os

hadoop_cmd="/usr/local/hadoop/bin/hadoop"#hadoop安装目录
data_root="/home/test_data/"
hdfs_root="/user/hadoop/"
taskid=3

def run_hadoopcmd(cmd):
    hadoopcmd = hadoop_cmd + " " + cmd
    os.system(hadoopcmd)

def get_all_path(local_cover_path,local_stego_path,local_txt_path):
#读取cover和stego文件夹下的所有图片名称，并且将名称对应的路径写到对应的txt文件中
    if os.path.exists(local_txt_path):
        os.system("rm -r "+local_txt_path)
    if not os.path.exists(local_txt_path):
        os.system("mkdir "+local_txt_path)
    i=0
    for root, dirs, files in os.walk(local_cover_path):
        for file in files:
            file_ext=os.path.splitext(file)[1]
            if  file_ext== '.jpg' or file_ext=='.jpeg':
                i=i+1
                content="0 "+hdfs_root+str(taskid)+"/cover/"+file
                txt_path=local_txt_path+str(i)+".txt"
                with open(txt_path, 'w') as f:
                    f.write(content)
    for root, dirs, files in os.walk(local_stego_path):
        for file in files:
            file_ext=os.path.splitext(file)[1]
            if  file_ext== '.jpg' or file_ext=='.jpeg':
                i=i+1
                content="1 "+hdfs_root+str(taskid)+"/stego/"+file
                txt_path=local_txt_path+str(i)+".txt"
                with open(txt_path, 'w') as f:
                    f.write(content)

def hadoop_52(local_cover_path,local_stego_path,taskid):  

    local_txt_path = data_root+str(taskid)+"/txt/"  

    get_all_path(local_cover_path,local_stego_path,local_txt_path)

    #写入到txt文件后，将图像文件和txt文件上传到hdfs上

    fs_task_path=hdfs_root+str(taskid)+"/"
    make_dir_path="fs -mkdir -p "+fs_task_path
    run_hadoopcmd(make_dir_path)

    copy_cover_cmd= "fs -copyFromLocal " + local_cover_path + " " + fs_task_path
    copy_stego_cmd= "fs -copyFromLocal " + local_stego_path + " " + fs_task_path
    copy_txt_cmd= "fs -copyFromLocal " + local_txt_path + " " + fs_task_path

    run_hadoopcmd(copy_cover_cmd)
    run_hadoopcmd(copy_stego_cmd)
    run_hadoopcmd(copy_txt_cmd)

    fs_task_path=hdfs_root+str(taskid)+"/"
    file_path = "/home/test_data/hh/mapper_train.py,/home/test_data/hh/reducer_train.py" 
    input_path = fs_task_path+"txt"
    output_path = fs_task_path + "output"

    run_hadoopcmd("jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar "\
                +"-D mapred.map.tasks=2 -D mapred.job.priority=HIGH  -D mapreduce.map.memory.mb=8192"\
		+" -files "+file_path+" -input " + input_path + " -output "+output_path \
		+" -mapper 'python mapper_train.py' -reducer 'python reducer_train.py'")




    local_output=data_root+str(taskid)+"/result"

    get_output="fs -get " + output_path + "/part-00000 " + local_output

    run_hadoopcmd(get_output)

    delete_cmd = "fs -rm -r " + fs_task_path

    run_hadoopcmd(delete_cmd)

    local_output=data_root+str(taskid)+"/result"
    local_cover_txt=data_root+str(taskid)+"/cover.txt"
    local_stego_txt=data_root+str(taskid)+"/stego.txt"
    c=[]
    s=[]
    with open(local_output,"r") as file_to_read:
        while True :
            lines=file_to_read.readline()
            if not lines:
                break
            line=lines.strip()
            words=line.split("\n")
            all=words[0].split(",")
            if(all[0]=="0"):
                c.append(all[1]+"\n")
            else:
                s.append(all[1]+"\n")
    with open(local_cover_txt, 'w') as f:

        f.writelines(c)
    with open(local_stego_txt, 'w') as f:

        f.writelines(s)


def main():

    local_cover_path = data_root+str(taskid)+"/cover/" 
    
    local_stego_path = data_root+str(taskid)+"/stego/"  

    local_txt_path = data_root+str(taskid)+"/txt/"  

    get_all_path(local_cover_path,local_stego_path,local_txt_path)

    #写入到txt文件后，将图像文件和txt文件上传到hdfs上

    fs_task_path=hdfs_root+str(taskid)+"/"
    make_dir_path="fs -mkdir -p "+fs_task_path
    run_hadoopcmd(make_dir_path)

    copy_cover_cmd= "fs -copyFromLocal " + local_cover_path + " " + fs_task_path
    copy_stego_cmd= "fs -copyFromLocal " + local_stego_path + " " + fs_task_path
    copy_txt_cmd= "fs -copyFromLocal " + local_txt_path + " " + fs_task_path

    run_hadoopcmd(copy_cover_cmd)
    run_hadoopcmd(copy_stego_cmd)
    run_hadoopcmd(copy_txt_cmd)

    fs_task_path=hdfs_root+str(taskid)+"/"
    file_path = "/home/test_data/hh/mapper_train.py,/home/test_data/hh/reducer_train.py" 
    input_path = fs_task_path+"txt"
    output_path = fs_task_path + "output"

    run_hadoopcmd("jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar "\
                +"-D mapred.map.tasks=2 -D mapred.job.priority=HIGH  -D mapreduce.map.memory.mb=8192"\
		+" -files "+file_path+" -input " + input_path + " -output "+output_path \
		+" -mapper 'python mapper_train.py' -reducer 'python reducer_train.py'")



    local_output=data_root+str(taskid)+"/result"
    get_output="fs -get " + output_path + "/part-00000 " + local_output
    run_hadoopcmd(get_output)

    delete_cmd = "fs -rm -r " + fs_task_path
    run_hadoopcmd(delete_cmd)

    local_output=data_root+str(taskid)+"/result"
    local_cover_txt=data_root+str(taskid)+"/cover.txt"
    local_stego_txt=data_root+str(taskid)+"/stego.txt"
    c=[]
    s=[]
    with open(local_output,"r") as file_to_read:
        while True :
            lines=file_to_read.readline()
            if not lines:
                break
            line=lines.strip()
            words=line.split("\n")
            all=words[0].split(",")
            if(all[0]=="0"):
                c.append(all[1]+"\n")
            else:
                s.append(all[1]+"\n")
    with open(local_cover_txt, 'w') as f:
        f.writelines(c)
    with open(local_stego_txt, 'w') as f:
        f.writelines(s)

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
