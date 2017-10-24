import os
import sys
import time
import math
import random
import csv
import multiprocessing
import linecache
import heapq
import numpy as np
from numpy import genfromtxt
from numpy import loadtxt
from scipy.spatial.distance import euclidean
from collections import defaultdict
INF = 999999


def npLoad(fileStr):
    temp = np.loadtxt(fileStr, delimiter=',')
    return temp

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
            oneDemdata[i] = (float(oneDemdata[i])-minnum)/(maxnum-minnum)
    return oneDemdata

def NormalizeDataset(dataset):
    maxnum = [-1000 for x in range(len(dataset[0]))]
    minnum = [100000 for x in range(len(dataset[0]))]
    for i in range(len(dataset)):
        for j in range(len(dataset[i])):
            #if j == 0:
                #continue
            if float(dataset[i][j]) >= maxnum[j]:
                maxnum[j] = float(dataset[i][j])
            if float(dataset[i][j]) <= minnum[j]:
                minnum[j] = float(dataset[i][j])

        #PRINT Dataset[0]
        #print maxnum
        #print minnum

        for i in range(len(dataset)):
            for j in range(len(dataset[i])):
                #if j == 0:
                            #continue
                if maxnum[j] == minnum[j]:
                    dataset[i][j] = 0
                else:
                    dataset[i][j] = (float(dataset[i][j]) - minnum[j]) / (maxnum[j] - minnum[j])

        return dataset

def SplitDataset(dataset):
    SplittedDataset = []
    i = 0
    while i < len(dataset):
        j = i+100
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
    #ard = [0 for i in range(hash_num)]
    for i in range(hash_num):
        b[i] = np.random.uniform(0, W)
        #ard[i] =  np.random.randint(0, MAX_HASH_RND)
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
    for q in query: #query is the #8 point
        q_hit_above_T[q] = []
        for j in range(len(hitTable[hitparaIndex[q]])): #array start with 0 while point index start with 1
            if hitTable[hitparaIndex[q]][j] >= T:
                q_hit_above_T[q].append(IndexPara[j])
    return q_hit_above_T

def getHitAboveTWithFilter(query, hitTable, dataset, paraIndex, IndexPara, hitparaIndex):
    q_hit_above_T = dict()
    for q in query: #query is the #8 point
        q_hit_above_T[q] = []
        for j in range(len(hitTable[hitparaIndex[q]])): #array start with 0 while point index start with 1
            if hitTable[hitparaIndex[q]][j] >= T and euclidean(dataset[paraIndex[q]], dataset[int(j)]) <= R:
                q_hit_above_T[q].append(IndexPara[j])
    return q_hit_above_T

def convertPointToDoc(point):
    return para_to_doc_dict[point]

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
    topN = math.ceil(len(docMins) * (1-precent))
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
    num = stop-start
    MHDTotal = 0
    for i in range(int(num)):
        MHDTotal += docMins[int(i + start)]
    return MHDTotal/num


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
    num = stop-start
    MHDTotal = 0
    for i in range(int(num)):
        MHDTotal = docMins[int(i + start)]
    return MHDTotal / num




def getMHDbyqLSH(q, doc, hitTable, dataset, paraIndex, hitparaIndex): #Computer the MHD bound matrix
    hitValueofDoc = []
    for p in doc_to_para_dict[doc]:
        if paraIndex[p] >= NP:
            continue
        hitValueofDoc.append((p, hitTable[hitparaIndex[q]][paraIndex[p]]))
    hitValueofDoc.sort(key=lambda tup: tup[1], reverse=True)
    top5hits = [x[0] for x in hitValueofDoc][:5]   #here set as 5
    minium = INF
    #the top value can be changed, set top5 first
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

#queryDic: the ininal dic generate before Prun
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
        dist.sort(reverse=True) # sort MHD bound matrix from large to small

        topN = math.ceil(len(dist) * (1 - precent))
        if precent == 1:
            topN = 1
        totalDis = sum(dist[:int(topN)])
        distFin = totalDis/topN
        disList.append((doc, distFin))
    disList.sort(key=lambda tup: tup[1])
    sortedDocList = [x[0] for x in disList]
    #print sortedDocList, len(sortedDocList)
    return sortedDocList


#queryDic: the ininal dic generate before Prun (M2HD)
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
        dist.sort(reverse=True) # sort MHD bound matrix from large to small
        start = math.ceil(len(dist)*begin)
        stop = math.ceil(len(dist)*end)
        num = stop-start
        totalDis = sum(dist[int(start): int(stop)])
        distFin = totalDis/num
        disList.append((doc, distFin))
    disList.sort(key=lambda tup: tup[1])
    sortedDocList = [x[0] for x in disList]
    #print sortedDocList, len(sortedDocList)
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
        top5hits = [x[0] for x in hitValueofDoc][:5]   #here set as 5
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
        distReverList.append((x[0], topKList[1][1]/pow(x[1], 5)))#HERE modify better distribution function
    probList = dict()
    sumofrDis = sum([x[1] for x in distReverList])
    for x in topKList:
        probList[doc_to_au_dict[x[0]]] = 0
    for tup in distReverList:
        au = doc_to_au_dict[tup[0]]
        prob = tup[1]/sumofrDis
        probList[au] = probList[au] + prob
    probList = sorted(probList.items(), key=lambda x: x[1], reverse=True)
    return probList

def multiprocessLoad(folerStr):
    fileNameList = os.listdir(folerStr)
    fileNameList.sort()
    dirString = folerStr+"/"
    fileNameList = [dirString + s  for s in fileNameList]
    if __name__ == "__main__":
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(pool_size)
        results = pool.map(npLoad, fileNameList,)
        pool.close()
        pool.join()
    my_data = combinedata(results)
    return my_data

def multiprocessNorm(dataset):
    if __name__ == "__main__":
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(pool_size)
        results = pool.map(NormalizeOneDem, dataset.T,)
        pool.close()
        pool.join()
#       normData = combinedata(results)
    return np.asarray(results).T
