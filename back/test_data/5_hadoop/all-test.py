# coding=utf-8
# !/usr/bin/env python
import sys
import subprocess



def main():
    test_predict="python hadoop_predict.py"
    test_train="python hadoop_train.py"
    test_train_predict="python hadoop_train_predict.py"
    test_predict_SDA="python hadoop_predict_SDA.py"
    test_train_SDA="python hadoop_train_SDA.py"
    test_train_predict_SDA="python hadoop_train_predict_SDA.py"
    for i in range(0,1):
        subprocess.run(test_predict,shell=True)
        subprocess.run(test_train,shell=True)
        subprocess.run(test_train_predict,shell=True)
        subprocess.run(test_predict_SDA,shell=True)
        subprocess.run(test_train_SDA,shell=True)
        subprocess.run(test_train_predict_SDA,shell=True)
    
if __name__ == '__main__':
    main()
