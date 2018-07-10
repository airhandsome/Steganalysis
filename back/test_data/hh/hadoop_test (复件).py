# coding=utf-8
# !/usr/bin/env python
import sys
import os

hadoop_cmd="/usr/local/hadoop/bin/hadoop"#hadoop安装目录
data_root="/home/test_data/"
hadfs_root="/user/hadoop"

def run_hadoopcmd(cmd):
    hadoopcmd = hadoop_cmd + " " + cmd
    os.system(hadoopcmd)

def getimgpath(local_img_path, local_txt_path):
    #读取文件夹下的所有图片名称，并且将名称对应的路径写到对应的txt文件中
    L=[]
    for root, dirs, files in os.walk(local_img_path):
        for file in files:
            file_ext=os.path.splitext(file)[1]
            if  file_ext== '.jpg' or file_ext=='.jpeg':
                L.append(file+"\n")
    #开始将L中的内容写入到txt文件中
    with open(local_txt_path, 'w') as f:
        f.writelines(L)


def main():
    local_img_path = data_root+"1"  # 存储所有图片的文件夹
    local_txt_path = data_root+"imgpath.txt"  # 储存所有图片名的txt文件
    getimgpath(local_img_path, local_txt_path)
    #写入到txt文件后，将图像文件和txt文件上传到hdfs上
    fs_txt_path=hadfs_root+"/data/txt"
    fs_img_path=hadfs_root+"/data/img"
    delete_cmd = "fs -rm -r " + fs_txt_path
    put_cmd="fs -mkdir -p "+fs_txt_path
    run_hadoopcmd(delete_cmd)
    run_hadoopcmd(put_cmd)
    delete_cmd = "fs -rm -r " + fs_img_path
    put_cmd="fs -put "+local_txt_path+" "+fs_txt_path
    run_hadoopcmd(delete_cmd)
    run_hadoopcmd(put_cmd)
    print('delete fileDIr successful')
    #将图像上传到hdfs中
    copydata_cmd= "fs -copyFromLocal " + local_img_path + " " + fs_img_path
    run_hadoopcmd(copydata_cmd)
    print('Copy from local successful')

    #开始准备mapper reducer
    file_path = "/home/test_data/hh/mapper.py,/home/test_data/hh/reducer.py" 
    input_path = "/user/hadoop/data/txt "
    output_path = "/user/hadoop/data/output"
    delete_cmd = "fs -rm -r " + output_path
    run_hadoopcmd(delete_cmd)
    run_hadoopcmd("jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar "\
		+" -files "+file_path+" -input " + input_path + " -output "+output_path \
		+" -mapper 'python mapper.py' -reducer 'python reducer.py'")
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
