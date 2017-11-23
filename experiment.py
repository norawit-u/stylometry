import os
import sys
import time
import math
import random
import csv
import argparse
import multiprocessing
import linecache
import heapq
import stylometry
import numpy as np
from numpy import genfromtxt
from numpy import loadtxt
from scipy.spatial.distance import euclidean
from collections import defaultdict

parser = argparse.ArgumentParser(description='Runing the experiment')
parser.add_argument('--csv_path', type=str, help='path of the csv')
parser.add_argument('--output_path', type=str, help='output path after running experiment')
parser.add_argument('--num_fragment', type=str, help='Number of fragment')
arg = parser.parse_args()  # get argparse argument
INF = 999999

output_dir = 'out'
syn_name = arg.csv_path
fragment_total = arg.num_fragment
directory = arg.output_path + '/' + syn_name

if not os.path.exists(directory):
    os.makedirs(directory)

print("reading file")
start = time.time()
print(arg.csv_path)
my_data = stylometry.npLoad(arg.csv_path)  # input dataset
print((time.time() - start, "used to read file"))

# parameters
D = 56  # dimensions
L = 200  # the number of group hash
K = 3  # the number of hash functions in each group hash
# N=30000 # the size of dataset
# N=4259934 # the size of dataset
N = len(my_data)  # the size of dataset
NP = len(my_data)  # the size of data used for QP
# NP = 4259934 #the size of data used for QP
# NP = 30000 #the size of data used for QP
NDocQ = 5
R = 0.12 * math.sqrt(D)  # query range
W = 1.2  # the width of bucket
T = 20  # collision threshold

MHDRatio = 0.5  # the MHD precentage
# M2LSHrange: from large to small based on mindist
startp = 0.25
endp = 0.5

topknn = 21  # MHD TopN list length
shdTopN = 21  # SHD TopN list length
flagNum = 3  # MHD TopN flag for pruning method(after flag times, stop..)

querySet = [x for x in range(1, int(fragment_total) + 1)]
# print(querySet)
print(my_data.shape)
print("indexing")
start = time.time()

doc_id = my_data[:, 0]  # fragment id
author_id = my_data[:, 1]  # auther_id
para_id = my_data[:, 2]  # chunk id
dataset = my_data[:, 3:]  # full features

# generate Indexes
para_to_doc_dict = dict(list(zip(para_id, doc_id)))
doc_to_au_dict = dict(list(zip(doc_id, author_id)))
para_to_au_dict = dict(list(zip(para_id, author_id)))
paraIndex = dict()
IndexPara = dict()

for i in range(len(para_id)):
    paraIndex[para_id[i]] = i
for i in range(len(para_id)):
    IndexPara[i] = para_id[i]

doc_to_para_dict = defaultdict(list)
for key, value in sorted(para_to_doc_dict.items()):
    doc_to_para_dict[value].append(key)
doc_to_para_dict = dict(doc_to_para_dict)

au_to_para_dict = defaultdict(list)
for key, value in sorted(para_to_au_dict.items()):
    au_to_para_dict[value].append(key)
au_to_para_dict = dict(au_to_para_dict)

au_to_doc_dict = defaultdict(list)
for key, value in sorted(doc_to_au_dict.items()):
    au_to_doc_dict[value].append(key)
au_to_doc_dict = dict(au_to_doc_dict)
print((time.time() - start, "seconds used to index"))

# normalize data
start = time.time()
# datasetm = stylometry.NormalizeDataset(dataset)
datasetm = stylometry.multiprocessNorm(dataset)
datasetQ = []
datasetP = []
datasetN = []
print((time.time() - start, "seconds used to normalize"))
for i in range(N):
    datasetN.append(datasetm[i])

for i in range(NP):
    datasetP.append(datasetm[i])

hitparaIndex = dict()
hitIndexPara = dict()

for doc in querySet:
    try:
        paraList = doc_to_para_dict[doc]
        for para in paraList:
            datasetQ.append(datasetm[paraIndex[para]])
            hitparaIndex[para] = (len(datasetQ) - 1)
            hitIndexPara[(len(datasetQ) - 1)] = para
    except KeyError:
        continue

import _LSH as lsh

l = lsh.new_LSH(len(datasetP), len(datasetQ), D, L, K, W, T)
print("move setN")
lsh.LSH_setSetN(l, datasetP)
print("move setQ")
lsh.LSH_setSetQ(l, datasetQ)
lsh.LSH_setThreadMode(l, 3)
print((lsh.LSH_reportStatus(l)))
print("start collision counting")
combinedhitQP = lsh.LSH_getCollisionMatrix(l)
lsh.LSH_clear(l)
lsh.delete_LSH(l)

sys.getsizeof(combinedhitQP)
print("lsh part finished")
print((len(combinedhitQP)))
print((len(combinedhitQP[0])))
print((combinedhitQP[0][0]))


# query processing


def queryExp(q):
    try:
        queryParaList = doc_to_para_dict[q]
    except KeyError:
        return
    data = ""
    queryParaList = doc_to_para_dict[q]
    print(("DocID", q))
    #    data += (str(q) + "\n")

    try:

        """
        #LSH Pruning SHD
        start = time.time()
        q_hit_above_T = getHitAboveT(queryParaList, combinedhitQP, hitparaIndex, IndexPara)
        doc_hit_above_T_dict = convertQdicttoDocdict(queryParaList, q_hit_above_T)
        LSHLongListTime = time.time()-start
        print "Time to generate long doc_list (LSH+SHD) ", LSHLongListTime
#        data += (str(LSHLongListTime) + "\n")


        start = time.time()
        ListLSH_SHDPrun = getSHDPrunedDoc(queryParaList, doc_hit_above_T_dict)
        LSHSHDPrunListTime = time.time()-start
        print "Pruning Time (LSH+SHD): ", LSHSHDPrunListTime
#        data += (str(LSHSHDPrunListTime) + "\n")

        start = time.time()
        LSH_SHD_list, LSH_SHD_values = getSHDTop5Doc(ListLSH_SHDPrun, queryParaList, doc_to_para_dict, shdTopN, combinedhitQP, datasetP, paraIndex, hitparaIndex, "lsh")
        LSH_SHDTop5ListTime = time.time() - start
        print "Time to return topk doclist (LSH+SHD):", LSH_SHDTop5ListTime
#        data += (str(LSH_SHDTop5ListTime) + "\n")

        """
        # LSH Pruning MHD
        start = time.time()
        q_hit_above_T = stylometry.getHitAboveT(queryParaList, combinedhitQP, hitparaIndex, IndexPara)
        doc_hit_above_T_dict = stylometry.convertQdicttoDocdict(queryParaList, q_hit_above_T)
        ListLSH_ALL = stylometry.getAllDoc(queryParaList, doc_hit_above_T_dict)
        LSHMHDLongListTime = time.time() - start
        print(("Time to generate long doc_list (LSH+MHD) ", LSHMHDLongListTime))
        #        data += (str(LSHMHDLongListTime) + "\n")

        start = time.time()
        sortedListLSH = stylometry.MHDPrunList(queryParaList, ListLSH_ALL, combinedhitQP, datasetm, paraIndex, R,
                                               hitparaIndex, doc_hit_above_T_dict, "lsh", MHDRatio)
        print(("MHDPrunList %s" % (time.time() - start)))
        LSH_MHD_Result, LSH_MHD_values = stylometry.geneMHDPrun(sortedListLSH, queryParaList, combinedhitQP, topknn,
                                                                flagNum, datasetm, paraIndex, hitparaIndex, MHDRatio,
                                                                "lsh")
        print(("geneMHDPrun %s" % (time.time() - start)))

        LSH_MHD_len = LSH_MHD_Result[0]
        LSH_MHD_list = LSH_MHD_Result[1]
        LSHMHDgenTop5ListTime = time.time() - start
        print(("Time to return topk doclist (LSH+MHD):", LSHMHDgenTop5ListTime))
        #        data += (str(LSHMHDgenTop5ListTime) + "\n")
        """
        #LSH Pruning M2HD
        start = time.time()
        q_hit_above_T = getHitAboveT(queryParaList, combinedhitQP, hitparaIndex, IndexPara)
        doc_hit_above_T_dict = convertQdicttoDocdict(queryParaList, q_hit_above_T)
        ListLSH_ALL = getAllDoc(queryParaList, doc_hit_above_T_dict)
        LSHMHDLongListTime = time.time()-start
        print "Time to generate long doc_list (LSH+M2HD) ", LSHMHDLongListTime
#        data += (str(LSHMHDLongListTime) + "\n")

        start = time.time()
        sortedListLSH = M2HDPrunList(queryParaList, ListLSH_ALL, combinedhitQP, datasetm, paraIndex, R, hitparaIndex, doc_hit_above_T_dict, "lsh", startp, endp)
        LSH_M2HD_Result, LSH_M2HD_values = geneM2HDPrun(sortedListLSH, queryParaList, combinedhitQP, topknn, flagNum, datasetm, paraIndex, hitparaIndex, "lsh", startp, endp)
        LSH_M2HD_len = LSH_M2HD_Result[0]
        LSH_M2HD_list = LSH_M2HD_Result[1]
        LSHM2HDgenTop5ListTime = time.time() -start
        print "Time to return topk doclist (LSH+M2HD):", LSHM2HDgenTop5ListTime
 #       data += (str(LSHM2HDgenTop5ListTime) + "\n")
        #data +=str(LSH_MHD_Result)+"\n"
        print "docList Length LSH: ", len(ListLSH_ALL)
  #      data += (str(len(ListLSH_ALL)) + "\n")
        print "docList Length LSH_SHD: ", len(ListLSH_SHDPrun)
  #      data += (str(ListLSH_SHDPrun) + "\n")
  #      data += (str(LSH_MHD_len) + "\n")
        print "docList Length LSH_MHD: ", LSH_MHD_len
  #      data += (str(LSH_M2HD_len) + "\n")
        # print "docList Length LSH_M2HD: ", LSH_M2HD_len
  #      data += (str(len(ListLSH_ALL)/len(ListLSH_SHDPrun)) + "\n")
        print "prunRatio LSH_SHD: ", len(ListLSH_ALL)/len(ListLSH_SHDPrun)
 #       data += (str(len(ListLSH_ALL)/LSH_MHD_len) + "\n")
        print "prunRatio LSH_MHD: ", len(ListLSH_ALL)/LSH_MHD_len
 #       data += (str(len(ListLSH_ALL)/LSH_M2HD_len) + "\n")
        # print "prunRatio LSH_M2HD: ", len(ListLSH_ALL)/LSH_M2HD_len

        def print_authorid(final_list):
            if len(final_list)==1:
                print "there is only one doc"
            else:
                print doc_to_au_dict[final_list[1]]

        print "///////////////////////////////////////////"
        print "use the top2 closet document to determine author_id:"
        print "Origin author: ", doc_to_au_dict[q]
        data += (str(doc_to_au_dict[q]) + "\n")


        LSH_SHD_PKNN = PKNN(queryParaList, LSH_SHD_list, "LSH_SHD","", 20, 0, combinedhitQP, 5, datasetP, paraIndex )
        print "LSH_SHD PKNN: ", LSH_SHD_PKNN
#        data += (str(LSH_SHD_PKNN) + "\n")

        LSH_MHD_PKNN = PKNN(queryParaList, LSH_MHD_list, "LSH_MHD","", 20, 0, combinedhitQP, 5, datasetP, paraIndex )
        print "LSH_MHD PKNN: ", LSH_MHD_PKNN
       data += (str(LSH_MHD_PKNN) + "\n") # PMF of the results
        """
        data += (str(LSH_MHD_values) + "\n")  # distance and segment id (0.5, 554.0)
        """
        LSH_M2HD_PKNN = PKNN(queryParaList, LSH_MHD_list, "LSH_M2HD","", 20, 0, combinedhitQP, 5, datasetP, paraIndex )
        print "LSH_M2HD PKNN: ", LSH_M2HD_PKNN
        data += (str(LSH_M2HD_PKNN) + "\n")
        """

        def changeDocTODocAuTuple(docList):
            result = []
            for doc in docList:
                result.append((doc, doc_to_au_dict[doc]))
            return result

        fileNameString = "./" + output_dir + '/' + syn_name + "/" + str(q)
        f = open(fileNameString, 'w')
        f.write(data)
        f.close()
        print("I am gonna write the result in the directory please check it whether it is okaaaaaaaaaaay")
    except:
        raise


if __name__ == '__main__':
    start_time = time.time()
    pool_size = multiprocessing.cpu_count()
    processNum = 12  # input("please input processors num")
    print("start pooling")
    pool = multiprocessing.Pool(processes=processNum, )
    temp = pool.map(queryExp, querySet, )
    pool.close()
    pool.join()
    end_time = time.time() - start_time
    print(end_time)
