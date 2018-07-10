__author__ = 'fly'

import sys
import pickle

import numpy as np
import math


def readModel(modelFile):
    try:
        print(modelFile)
        mf=open(modelFile,'rb')
        modle=pickle.load(mf)
        print("read model success")

    except Exception as e:
        print("readModel exception")
        print(e)

    finally:
        mf.close()

    return modle

def savePredict(base_learner_num,votes,outFile):
    try:
        out=open(outFile,'w')
        out.write("lable cover stego\n")
        num=votes.shape[0]
        label='0'

        for i in range(num):
            vote=votes[i][0]+base_learner_num
            stego_vote=vote/2
            cover_vote=base_learner_num-stego_vote
            if(cover_vote>=stego_vote):
                label='0'
            else:
                label='1'

            cover_rate=float(cover_vote)/base_learner_num
            stego_rate=1-cover_rate
            out.write(label+' '+str(cover_rate)+' '+str(stego_rate)+'\n')

    except Exception as e:
        print("savePredict exception")
        print(e)

    finally:
        out.close()

def readtxt(path):
    file = open(path,'r')
    res = []
    for line in file.readlines():
        tmp = []
        lines = line.strip().split(' ')
        if len(lines) < 2: continue
        for x in lines:
            tmp.append(float(x))
        res.append(tmp)
    file.close()
    return np.array(res)

def predict(modelFile,featureFile,outFile):
    base_learners=readModel(modelFile)
    feature = readtxt(featureFile)
    # feature = ga.to_gpu(cpu_feature)
    # featrue=np.loadtxt(featureFile,dtype=np.float32, delimiter=' ')
    featrueNum = feature.shape[0]

    # rw=open(outFile,'w')
    base_learner_num=len(base_learners)
    votes=np.zeros((featrueNum,1),dtype=np.float32)

    for i in range(1,base_learner_num+1):
        base_learner=base_learners[i]
        proj=feature[:,base_learner['subspace']].dot(base_learner['w'])-base_learner['b']
        votes=votes+np.sign(proj)


    votes[votes==0]=np.random.rand(np.sum(votes==0))-0.5
    # results=np.sign(votes)
    # print "predict finish"
    savePredict(base_learner_num,votes,outFile)




if __name__=='__main__':
     if len(sys.argv)!=4:
         print("wrong argv")
     else:
         modelFile=sys.argv[1]
         featrueFile=sys.argv[2]
         outFile=sys.argv[3]
         predict(modelFile,featrueFile,outFile)
