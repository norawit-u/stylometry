print(__doc__)
import time
import numpy as np
import argparse
# from sklearn import preprocessing
# from sklearn.datasets.samples_generator import make_blobs
from collections import defaultdict
import math
import random
import multiprocessing
from numpy import genfromtxt
from numpy import loadtxt
import linecache
import heapq
from scipy.spatial.distance import euclidean
import csv
import os
import sys

INF = 999999

parser = argparse.ArgumentParser(description='Runing the experiment')
parser.add_argument('--input', type=str, help='path of the csv')
parser.add_argument('--output_path', type=str, help='output path after running experiment')
parser.add_argument('--num_fragment', type=str, help='Number of fragment')
arg = parser.parse_args()  # get argparse argument
INF = 999999

output_dir = 'out'
syn_name = arg.input
fragment_total = int(arg.num_fragment)
directory = arg.output_path + '/' + syn_name.split('/')[-1].split('.')[0]

if not os.path.exists(directory):
    os.makedirs(directory)

output_dir = directory

def npLoad(fileStr):
    temp = np.genfromtxt(fileStr, delimiter=',')
    return np.delete(temp, 0, 0)


def NormalizeOneDem(oneDemdata):
    maxnum = -1000
    minnum = 100000
    for i in range(len(oneDemdata)):
        if float(oneDemdata[i]) >= maxnum:
            maxnum = float(oneDemdata[i])
        if float(oneDemdata[i]) <= minnum:
            minnum = float(oneDemdata[i])
    for i in range(len(oneDemdata)):
        if maxnum == minnum:
            oneDemdata[i] = 0
        else:
            oneDemdata[i] = (float(oneDemdata[i]) - minnum) / (maxnum - minnum)
    return oneDemdata


def NormalizeDataset(dataset):
    maxnum = [-1000 for x in range(len(dataset[0]))]
    minnum = [100000 for x in range(len(dataset[0]))]
    for i in range(len(dataset)):
        for j in range(len(dataset[i])):
            # if j == 0:
            # continue
            if float(dataset[i][j]) >= maxnum[j]:
                maxnum[j] = float(dataset[i][j])
            if float(dataset[i][j]) <= minnum[j]:
                minnum[j] = float(dataset[i][j])

    # PRINT Dataset[0]
    # print maxnum
    # print minnum

    for i in range(len(dataset)):
        for j in range(len(dataset[i])):
            # if j == 0:
            # continue
            if maxnum[j] == minnum[j]:
                dataset[i][j] = 0
            else:
                dataset[i][j] = (float(dataset[i][j]) - minnum[j]) / (maxnum[j] - minnum[j])

    return dataset


def SplitDataset(dataset):
    SplittedDataset = []
    i = 0
    while i < len(dataset):
        j = i + 100
        splitdataset = dataset[i:j]
        SplittedDataset.append(splitdataset)
        i = j
    return SplittedDataset


def generateA(hash_num, D):
    a = [0 for x in range(hash_num)]
    for i in range(hash_num):
        a[i] = np.random.normal(0.5, 0.5, D)
    return a


def generateA_new(hash_num, K, D):
    a = [[0 for x in range(K)] for y in range(hash_num)]
    for i in range(hash_num):
        for j in range(K):
            a[i][j] = np.random.normal(0.5, 0.5, D)
            for d in range(D):
                a[i][j][d] = abs(a[i][j][d])
    return a


def generateB(hash_num, W):
    b = [0 for i in range(hash_num)]
    # ard = [0 for i in range(hash_num)]
    for i in range(hash_num):
        b[i] = np.random.uniform(0, W)
        # ard[i] =  np.random.randint(0,MAX_HASH_RND)
    return b


def combinedata(result):
    CombinedHash = []
    for i in range(len(result)):
        for j in range(len(result[i])):
            CombinedHash.append(result[i][j])
    return CombinedHash


def genDisSmallerThenRSet(query, dataset, paraIndex):
    q_dis_smaller_R = dict()
    for q in query:
        q_dis_smaller_R[q] = []
        for d in range(len(dataset)):
            if euclidean(dataset[paraIndex[q]], dataset[d]) <= R:
                q_dis_smaller_R[q].append(IndexPara[d])
    return q_dis_smaller_R


def getHitAboveT(query, hitTable, hitparaIndex, IndexPara):
    q_hit_above_T = dict()
    for q in query:  # query is the #8 point
        q_hit_above_T[q] = []
        for j in range(len(hitTable[hitparaIndex[q]])):  # array start with 0 while point index start with 1
            if hitTable[hitparaIndex[q]][j] >= T:
                q_hit_above_T[q].append(IndexPara[j])
    return q_hit_above_T


def getHitAboveTWithFilter(query, hitTable, dataset, paraIndex, IndexPara, hitparaIndex):
    q_hit_above_T = dict()
    for q in query:  # query is the #8 point
        q_hit_above_T[q] = []
        for j in range(len(hitTable[hitparaIndex[q]])):  # array start with 0 while point index start with 1
            if hitTable[hitparaIndex[q]][j] >= T and euclidean(dataset[paraIndex[q]], dataset[int(j)]) <= R:
                q_hit_above_T[q].append(IndexPara[j])
    return q_hit_above_T


def convertPointToDoc(point):
    return para_to_doc_dict[point];


def convertQdicttoDocdict(query, Qdict):
    queryWithSelectedDoc = dict()
    for q in query:
        selectedDoc = list()
        for para in Qdict[q]:
            selectedDoc.append(convertPointToDoc(para))
        queryWithSelectedDoc[q] = list(set(selectedDoc))
    return queryWithSelectedDoc


def getAllDoc(query, Qdict):
    result = Qdict[query[0]]
    for q in query:
        result = list(set(result).union(set(Qdict[q])))
    return result


def getSHDPrunedDoc(query, Qdict):
    result = Qdict[query[0]]
    for q in query:
        result = list(set(result).intersection(set(Qdict[q])))
    return result


def getLSHMHD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, precent):
    docMins = []
    for query_point in query:
        minium = getMHDbyqLSH(query_point, doc, hitTable, dataset, paraIndex, hitparaIndex)
        docMins.append(minium)
    docMins.sort(reverse=True)
    topN = math.ceil(len(docMins) * (1 - precent))
    if precent == 1:
        topN = 1
    MHDTotal = 0
    for i in range(int(topN)):
        MHDTotal += docMins[i]
    return MHDTotal / topN


def getLSHM2HD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, begin, end):
    docMins = []
    for query_point in query:
        minium = getMHDbyqLSH(query_point, doc, hitTable, dataset, paraIndex, hitparaIndex)
        docMins.append(minium)
    docMins.sort(reverse=True)
    start = math.ceil(len(docMins) * begin)
    stop = math.ceil(len(docMins) * end)
    num = stop - start
    MHDTotal = 0
    for i in range(int(num)):
        MHDTotal += docMins[int(i + start)]
    return MHDTotal / num


def getBFMHD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, precent):
    docMins = []
    for query_point in query:
        minium = getMHDbyqBF(query_point, doc, dataset, paraIndex)
        docMins.append(minium)
    docMins.sort(reverse=True)
    topN = math.ceil(len(docMins) * (1 - precent))
    MHDTotal = 0
    for i in range(int(topN)):
        MHDTotal += docMins[i]
    return MHDTotal / topN


def getBFM2HD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, begin, end):
    docMins = []
    for query_point in query:
        minium = getMHDbyqBF(query_point, doc, dataset, paraIndex)
        docMins.append(minium)
    docMins.sort(reverse=True)
    start = math.ceil(len(docMins) * begin)
    stop = math.ceil(len(docMins) * end)
    num = stop - start
    MHDTotal = 0
    for i in range(int(num)):
        MHDTotal += docMins[int(i + start)]
    return MHDTotal / num


def getMHDbyqLSH(q, doc, hitTable, dataset, paraIndex, hitparaIndex):  # Computer the MHD bound matrix
    hitValueofDoc = []
    for p in doc_to_para_dict[doc]:
        if paraIndex[p] >= NP:
            continue
        hitValueofDoc.append((p, hitTable[hitparaIndex[q]][paraIndex[p]]))
    hitValueofDoc.sort(key=lambda tup: tup[1], reverse=True)
    top5hits = [x[0] for x in hitValueofDoc][:5]  # here set as 5
    minium = INF
    # the top value can be changed, set top5 first
    for point in top5hits:
        dis = euclidean(dataset[paraIndex[q]], dataset[paraIndex[point]])
        if dis < minium:
            minium = dis
    return minium


def getMHDbyqBF(q, doc, dataset, paraIndex):
    minium = INF
    for p in doc_to_para_dict[doc]:
        if paraIndex[p] >= NP:
            continue
        temp = euclidean(dataset[paraIndex[q]], dataset[paraIndex[p]])
        if temp < minium:
            minium = temp
    return minium


# queryDic: the ininal dic generate before Prun
def MHDPrunList(query, docList, hitTable, dataset, paraIndex, R, hitparaIndex, queryDic, method, precent):
    disList = []
    for doc in docList:
        dist = []
        for q in query:
            if doc in queryDic[q]:
                if method == "lsh":
                    dist.append(getMHDbyqLSH(q, doc, hitTable, dataset, paraIndex, hitparaIndex))
                else:
                    dist.append(getMHDbyqBF(q, doc, dataset, paraIndex))
            else:
                dist.append(R)
        dist.sort(reverse=True)  # sort MHD bound matrix from large to small

        topN = math.ceil(len(dist) * (1 - precent))
        if precent == 1:
            topN = 1
        totalDis = sum(dist[:int(topN)])
        distFin = totalDis / topN
        disList.append((doc, distFin))
    disList.sort(key=lambda tup: tup[1])
    sortedDocList = [x[0] for x in disList]
    # print sortedDocList, len(sortedDocList)
    return sortedDocList


# queryDic: the ininal dic generate before Prun (M2HD)
def M2HDPrunList(query, docList, hitTable, dataset, paraIndex, R, hitparaIndex, queryDic, method, begin, end):
    disList = []
    for doc in docList:
        dist = []
        for q in query:
            if doc in queryDic[q]:
                if method == "lsh":
                    dist.append(getMHDbyqLSH(q, doc, hitTable, dataset, paraIndex, hitparaIndex))
                else:
                    dist.append(getMHDbyqBF(q, doc, dataset, paraIndex))
            else:
                dist.append(R)
        dist.sort(reverse=True)  # sort MHD bound matrix from large to small
        start = math.ceil(len(dist) * begin)
        stop = math.ceil(len(dist) * end)
        num = stop - start
        totalDis = sum(dist[int(start): int(stop)])
        distFin = totalDis / num
        disList.append((doc, distFin))
    disList.sort(key=lambda tup: tup[1])
    sortedDocList = [x[0] for x in disList]
    # print sortedDocList, len(sortedDocList)
    return sortedDocList


def geneMHDPrun(docList, query, hitTable, topN, flagNum, dataset, paraIndex, hitparaIndex, precent, method):
    resultList = []
    stopFlag = 0
    count = 0
    for doc in docList:
        if stopFlag >= flagNum:
            break
        if method == "lsh":
            MHDDis = getLSHMHD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, precent)
        elif method == "bf":
            MHDDis = getBFMHD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, precent)
        count += 1
        if len(resultList) < topN:
            resultList.append((MHDDis, doc))
        else:
            resultList.sort(key=lambda tup: tup[0], reverse=True)
            topDis = resultList[0][0]
            if topDis > MHDDis:
                resultList[0] = (MHDDis, doc)
                stopFlag = 0
            else:
                stopFlag += 1
    resultList.sort(key=lambda tup: tup[0])
    result = [x[1] for x in resultList]
    return (count, result), resultList


def geneM2HDPrun(docList, query, hitTable, topN, flagNum, dataset, paraIndex, hitparaIndex, method, begin, end):
    resultList = []
    stopFlag = 0
    count = 0
    for doc in docList:
        if stopFlag >= flagNum:
            break
        if method == "lsh":
            MHDDis = getLSHM2HD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, begin, end)
        elif method == "bf":
            MHDDis = getBFM2HD(query, doc, hitTable, dataset, paraIndex, hitparaIndex, begin, end)
        count += 1
        if len(resultList) < topN:
            resultList.append((MHDDis, doc))
        else:
            resultList.sort(key=lambda tup: tup[0], reverse=True)
            topDis = resultList[0][0]
            if topDis > MHDDis:
                resultList[0] = (MHDDis, doc)
                stopFlag = 0
            else:
                stopFlag += 1
    resultList.sort(key=lambda tup: tup[0])

    result = [x[1] for x in resultList]
    return (count, result), resultList


def getLSHSHD(query, doc, doc_to_para_dict, hitTable, dataset, paraIndex, hitparaIndex):
    docMins = []
    for query_point in query:
        hitValueofDoc = []
        for p in doc_to_para_dict[doc]:
            if paraIndex[p] >= NP:
                continue
            hitValueofDoc.append((p, hitTable[hitparaIndex[query_point]][paraIndex[p]]))
        hitValueofDoc.sort(key=lambda tup: tup[1], reverse=True)
        top5hits = [x[0] for x in hitValueofDoc][:5]  # here set as 5
        minium = INF
        for p in top5hits:
            dis = euclidean(dataset[paraIndex[query_point]], dataset[paraIndex[p]])
            if dis < minium:
                minium = dis
        docMins.append(minium)
    maxium = 0
    for minDist in docMins:
        if minDist > maxium:
            maxium = minDist
    return maxium


def getBFSHD(query, doc, doc_to_para_dict, hitTable, dataset, paraIndex, hitparaIndex):
    docMins = []
    for query_point in query:
        minium = INF
        for p in doc_to_para_dict[doc]:
            if paraIndex[p] >= NP:
                continue
            dis = euclidean(dataset[paraIndex[query_point]], dataset[paraIndex[p]])
            if dis < minium:
                minium = dis
        docMins.append(minium)
    maxium = 0
    for minDist in docMins:
        if minDist > maxium:
            maxium = minDist
    return maxium


def getSHDTop5Doc(listOfdoc, query, doc_to_para_dict, shdTopN, hitTable, dataset, paraIndex, hitparaIndex, method):
    shdValues = []
    for d in listOfdoc:
        if method == "lsh":
            shdValues.append((d, getLSHSHD(query, d, doc_to_para_dict, hitTable, dataset, paraIndex, hitparaIndex)))
        else:
            shdValues.append((d, getBFSHD(query, d, doc_to_para_dict, hitTable, dataset, paraIndex, hitparaIndex)))
    shdValues.sort(key=lambda tup: tup[1])
    top5Doc = [x[0] for x in shdValues][:shdTopN]
    return top5Doc, shdValues


def PKNN(query, candiList, disFunc, weiFunc, k, beta, hitTable, topN, dataset, paraIndex):
    distList = []
    for c in candiList:
        if disFunc == "BF_SHD":
            dist = getBFSHD(query, c, doc_to_para_dict, hitTable, dataset, paraIndex, hitparaIndex)
        elif disFunc == "BF_MHD":
            dist = getBFMHD(query, c, hitTable, dataset, paraIndex, hitparaIndex, MHDRatio)
        elif disFunc == "BF_M2HD":
            dist = getBFM2HD(query, c, hitTable, dataset, paraIndex, hitparaIndex, startp, endp)
        elif disFunc == "LSH_SHD":
            dist = getLSHSHD(query, c, doc_to_para_dict, hitTable, dataset, paraIndex, hitparaIndex)
        elif disFunc == "LSH_MHD":
            dist = getLSHMHD(query, c, hitTable, dataset, paraIndex, hitparaIndex, MHDRatio)
        elif disFunc == "LSH_M2HD":
            dist = getLSHM2HD(query, c, hitTable, dataset, paraIndex, hitparaIndex, startp, endp)
        distList.append((c, dist))
    distList.sort(key=lambda tup: tup[1])
    topKList = distList[:k]
    distReverList = []
    for x in topKList:
        if x[1] == 0:
            continue
        distReverList.append((x[0], topKList[1][1] / pow(x[1], 5)))  # HERE modify better distribution function
    probList = dict()
    sumofrDis = sum([x[1] for x in distReverList])
    for x in topKList:
        probList[doc_to_au_dict[x[0]]] = 0
    for tup in distReverList:
        au = doc_to_au_dict[tup[0]]
        prob = tup[1] / sumofrDis
        probList[au] = probList[au] + prob
    probList = sorted(probList.items(), key=lambda x: x[1], reverse=True)
    return probList


def multiprocessLoad(folerStr):
    fileNameList = os.listdir(folerStr)
    fileNameList.sort()
    dirString = folerStr + "/"
    fileNameList = [dirString + s for s in fileNameList]
    if __name__ == "__main__":
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(pool_size)
        results = pool.map(npLoad, fileNameList, )
        pool.close()
        pool.join()
    my_data = combinedata(results)
    return my_data


def multiprocessNorm(dataset):
    if __name__ == "__main__":
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(pool_size)
        results = pool.map(NormalizeOneDem, dataset.T, )
        pool.close()
        pool.join()
    # normData = combinedata(results)
    return np.asarray(results).T


print("reading file")
start = time.time()
# my_data = []
# with open("../../dataset.csv") as f:
#       for line in f:
#               sliceline = line.replace("\n", "").replace("\r", "").split(",")
#              my_data.append(sliceline)
# my_data = genfromtxt('../../dataset.csv', delimiter=',')
my_data = npLoad(arg.input)  # input dataset
# my_data = np.asarray(multiprocessLoad("../../splitedFiles1000000"))
print((time.time() - start, "used to read file"))
# parameters
D = 56  # dimensions
L = 334  # the number of group hash
K = 1  # the number of hash functions in each group hash
# N=30000 # the size of dataset
# N=4259934 # the size of dataset
N = len(my_data)  # the size of dataset
NP = len(my_data)  # the size of data used for QP
# NP = 4259934 #the size of data used for QP
# NP = 30000 #the size of data used for QP
NDocQ = 5
R = 0.12 * math.sqrt(D)  # query range
W = 1  # the width of bucket
T = 112  # collision threshold

MHDRatio = 0.5  # the MHD precentage
# M2LSHrange: from large to small based on mindist
startp = 0.25
endp = 0.5

topknn = 21  # MHD TopN list length
shdTopN = 21  # SHD TopN list length
flagNum = 3  # MHD TopN flag for pruning method(after flag times, stop..)

# load query documents: fragment_id  to query
# doc=[]
# line=linecache.getlines("./fragment5000.csv")
# for i in range(len(line)):
#         sliceline = line[i].replace("\n", "").replace("\r", "").split(",")
#         doc.append(sliceline)

# querySet=[]
# for i in range(len(doc)):
#     for j in range(len(doc[i])):
#         doc[i][j]= int(doc[i][j])
#         querySet.append(doc[i][j])
querySet = [x for x in range(1, fragment_total + 1)]
print(querySet)

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

print(doc_to_para_dict)
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
# datasetm= NormalizeDataset(dataset)
datasetm = multiprocessNorm(dataset)
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
        raise

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
    #    data+= (str(q) + "\n")

    try:
        #         #LSH Pruning SHD
        #         start = time.time()
        #         q_hit_above_T = getHitAboveT(queryParaList, combinedhitQP,hitparaIndex, IndexPara)
        #         doc_hit_above_T_dict = convertQdicttoDocdict(queryParaList,q_hit_above_T)
        #         LSHLongListTime = time.time()-start
        #         print "Time to generate long doc_list (LSH+SHD) ",LSHLongListTime
        # #        data+= (str(LSHLongListTime) + "\n")
        #         start = time.time()
        #         ListLSH_SHDPrun = getSHDPrunedDoc(queryParaList, doc_hit_above_T_dict)
        #         LSHSHDPrunListTime = time.time()-start
        #         print "Pruning Time (LSH+SHD): ",LSHSHDPrunListTime
        # #        data+= (str(LSHSHDPrunListTime) + "\n")
        #         start = time.time()
        #         LSH_SHD_list, LSH_SHD_values = getSHDTop5Doc(ListLSH_SHDPrun, queryParaList, doc_to_para_dict, shdTopN,combinedhitQP, datasetP, paraIndex, hitparaIndex,"lsh")
        #         LSH_SHDTop5ListTime = time.time() - start

        #         print "Time to return topk doclist (LSH+SHD):",LSH_SHDTop5ListTime
        # #        data+= (str(LSH_SHDTop5ListTime) + "\n")

        # LSH Pruning MHD
        start = time.time()
        q_hit_above_T = getHitAboveT(queryParaList, combinedhitQP, hitparaIndex, IndexPara)
        doc_hit_above_T_dict = convertQdicttoDocdict(queryParaList, q_hit_above_T)
        ListLSH_ALL = getAllDoc(queryParaList, doc_hit_above_T_dict)
        LSHMHDLongListTime = time.time() - start
        print(("Time to generate long doc_list (LSH+MHD) ", LSHMHDLongListTime))
        #        data+= (str(LSHMHDLongListTime) + "\n")
        start = time.time()
        sortedListLSH = MHDPrunList(queryParaList, ListLSH_ALL, combinedhitQP, datasetm, paraIndex, R, hitparaIndex,
                                    doc_hit_above_T_dict, "lsh", MHDRatio)
        print(("MHDPrunList %s" % (time.time() - start)))
        LSH_MHD_Result, LSH_MHD_values = geneMHDPrun(sortedListLSH, queryParaList, combinedhitQP, topknn, flagNum,
                                                     datasetm, paraIndex, hitparaIndex, MHDRatio, "lsh")
        print(("geneMHDPrun %s" % (time.time() - start)))
        LSH_MHD_len = LSH_MHD_Result[0]
        LSH_MHD_list = LSH_MHD_Result[1]
        LSHMHDgenTop5ListTime = time.time() - start
        print(("Time to return topk doclist (LSH+MHD):", LSHMHDgenTop5ListTime))
        #        data+= (str(LSHMHDgenTop5ListTime) + "\n")

        #         #LSH Pruning M2HD
        #         start = time.time()
        #         q_hit_above_T = getHitAboveT(queryParaList, combinedhitQP,hitparaIndex, IndexPara)
        #         doc_hit_above_T_dict = convertQdicttoDocdict(queryParaList,q_hit_above_T)
        #         ListLSH_ALL = getAllDoc(queryParaList,doc_hit_above_T_dict)
        #         LSHMHDLongListTime = time.time()-start
        #         print "Time to generate long doc_list (LSH+M2HD) ",LSHMHDLongListTime
        # #        data+= (str(LSHMHDLongListTime) + "\n")
        #         start = time.time()
        #         sortedListLSH = M2HDPrunList(queryParaList,ListLSH_ALL,combinedhitQP, datasetm,paraIndex,R,hitparaIndex,doc_hit_above_T_dict,"lsh",startp, endp)
        #         LSH_M2HD_Result, LSH_M2HD_values = geneM2HDPrun(sortedListLSH,queryParaList,combinedhitQP,topknn, flagNum, datasetm, paraIndex, hitparaIndex, "lsh", startp, endp)
        #         LSH_M2HD_len = LSH_M2HD_Result[0]
        #         LSH_M2HD_list = LSH_M2HD_Result[1]

        #        LSHM2HDgenTop5ListTime = time.time() -start
        #        print "Time to return topk doclist (LSH+M2HD):",LSHM2HDgenTop5ListTime
        # #       data+= (str(LSHM2HDgenTop5ListTime) + "\n")
        #        #data+=str(LSH_MHD_Result)+"\n"
        #        print "docList Length LSH: ", len(ListLSH_ALL)
        #  #      data+= (str(len(ListLSH_ALL)) + "\n")
        #        print "docList Length LSH_SHD: ", len(ListLSH_SHDPrun)
        #  #      data+= (str(ListLSH_SHDPrun) + "\n")
        #  #      data+= (str(LSH_MHD_len) + "\n")
        #        print "docList Length LSH_MHD: ", LSH_MHD_len
        #  #      data+= (str(LSH_M2HD_len) + "\n")
        #        # print "docList Length LSH_M2HD: ", LSH_M2HD_len

        #  #      data+= (str(len(ListLSH_ALL)/len(ListLSH_SHDPrun)) + "\n")
        #        print "prunRatio LSH_SHD: ", len(ListLSH_ALL)/len(ListLSH_SHDPrun)
        # #       data+= (str(len(ListLSH_ALL)/LSH_MHD_len) + "\n")
        #        print "prunRatio LSH_MHD: ", len(ListLSH_ALL)/LSH_MHD_len

        # #       data+= (str(len(ListLSH_ALL)/LSH_M2HD_len) + "\n")
        #        # print "prunRatio LSH_M2HD: ", len(ListLSH_ALL)/LSH_M2HD_len

        #        def print_authorid(final_list):
        #            if len(final_list)==1:
        #                print "there is only one doc"
        #            else:
        #                print doc_to_au_dict[final_list[1]]

        #        print "///////////////////////////////////////////"
        #        print "use the top2 closet document to determine author_id:"
        #        print "Origin author: ", doc_to_au_dict[q]
        #        data+= (str(doc_to_au_dict[q]) + "\n")


        #         LSH_SHD_PKNN = PKNN(queryParaList,LSH_SHD_list, "LSH_SHD","",20,0,combinedhitQP, 5, datasetP,paraIndex )
        #         print "LSH_SHD PKNN: ",LSH_SHD_PKNN
        # #        data+= (str(LSH_SHD_PKNN) + "\n")

        # LSH_MHD_PKNN = PKNN(queryParaList,LSH_MHD_list, "LSH_MHD","",20,0,combinedhitQP, 5, datasetP,paraIndex )
        # print "LSH_MHD PKNN: ",LSH_MHD_PKNN
        #        data+= (str(LSH_MHD_PKNN) + "\n") # PMF of the results
        data += (str(LSH_MHD_values) + "\n")  # distance and segment id (0.5, 554.0)

        # LSH_M2HD_PKNN = PKNN(queryParaList,LSH_MHD_list, "LSH_M2HD","",20,0,combinedhitQP, 5, datasetP,paraIndex )
        # print "LSH_M2HD PKNN: ",LSH_M2HD_PKNN
        #      data+= (str(LSH_M2HD_PKNN) + "\n")

        def changeDocTODocAuTuple(docList):
            result = []
            for doc in docList:
                result.append((doc, doc_to_au_dict[doc]))
            return result

        fileNameString = "./" + output_dir + "/" + str(q)
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
    endtime = time.time() - start_time
