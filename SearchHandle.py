import pandas as pd
import readfromDB
import fuzzysearch
import json
import pymongo

from nltk.stem.porter import *
import pymysql
import os

import time
from collections import defaultdict

import threading

test_dict = {
    "term1": {
        "docid1": {
            "pos": [1, 2, 3, 4, 5, 6],
            "score": 0.123
        },
        "docid2": {
            "pos": [2, 3, 4, 5, 6, 7],
            "score": 0.456
        }
    },
    "term2": {
        "docid3": {
            "pos": [11, 22, 33, 44, 55, 66],
            "score": 0.789
        },
        "docid4": {
            "pos": [22, 33, 44, 55, 66, 77],
            "score": 0.987
        }
    },
    "term3": {
        "docid3": {
            "pos": [1, 2, 3, 4, 5, 6],
            "score": 0.789
        },
        "docid1": {
            "pos": [22, 33, 44, 55, 66, 77],
            "score": 0.987
        }
    }
}


class SqlCreator(object):
    def __init__(self):
        self.sqlhandle = readfromDB.handlerwithsql()

    def getConn(self):
        return self.sqlhandle

    def close(self):
        self.sqlhandle.close_session()


class QueryError(Exception):
    def __init__(self, err='QueryError'):
        Exception.__init__(self, err)


class SearchHandle(object):
    def __init__(self):
        """
        需要在這裏分別接入 mysql 和 dymanoDB 的數據庫 然後再進行操作
        """
        # self.total_comment = self.sqlhandle.read_count
        self.stemer = PorterStemmer()
        self.fuzzy = fuzzysearch.Fuzzy()

        print("Initilized successfully")

    # def getConnected(self):
    #     self.sqlhandle = readfromDB.handlerwithsql()
    #
    # def closeConnected(self):
    #     self.sqlhandle.close_session()

    def readFromMysql(self, sql, connector):
        comment_dict = connector.read2dict(sql)
        # self.sqlhandle.close_session()
        return comment_dict

    def readBM25(self, term, connector):
        """
        term : 需要查的term
        result  : dict 包含該term的所有信息
        """
        __term = [term]
        terminfo = connector.readTerm25(__term)
        return terminfo[term]

    def initTerm(self, term, connector):
        """
        term : 在組合之前需要初始化的term
        return : term 對應的DataFrame
        """
        # self.getConnected()
        result = self.readBM25(term, connector)
        if not bool(result):
            try:
                result = self.readBM25(self.fuzzy.bktreeSearch(term)[0][1])
            except IndexError:
                raise QueryError('QueryError')
        # self.closeConnected()

        return result

    def getOneResult(self, term_df):
        return term_df

    def getANDResult(self, term_df1, term_df2):
        result = (term_df1 + term_df2).dropna(axis=1)
        for index, row in result.iteritems():
            row['pos'].sort()
        return result

    def getNewAndResult(self, term1, term2):
        picklist = defaultdict(dict)
        if len(term1) < len(term2):
            for inx in term1.keys():
                if inx in term2:
                    picklist[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
                    picklist[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
            return picklist
        else:
            for inx in term2.keys():
                if inx in term1:
                    picklist[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
                    picklist[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
            return picklist

    def getANDNeiResult(self, term_df1, term_df2):
        subresult = self.getANDResult(term_df1, term_df2)
        result = pd.DataFrame()
        picklist = []
        for index, row in subresult.iteritems():
            for i in range(len(row["pos"]) - 2):
                if (row["pos"][i + 1] - row["pos"][i] == 1) and (row["pos"][i + 1] in term_df2[index]["pos"]) and (
                        row["pos"][i] in term_df1[index]["pos"]):
                    if index not in picklist:
                        picklist.append(index)
            result = subresult[picklist]

        return result

    def getNewNeiResult(self, term1, term2):
        picklist = defaultdict(dict)
        if len(term1) < len(term2):
            for inx in term1.keys():
                if inx in term2:
                    for pos in term2[inx]['pos']:
                        if (pos + 1) in term1[inx]['pos']:
                            picklist[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
                            picklist[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
            return picklist
        else:
            for inx in term2.keys():
                if inx in term1:
                    for pos in term1[inx]['pos']:
                        if (pos + 1) in term2[inx]['pos']:
                            picklist[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
                            picklist[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
            return picklist

    def getXORResult(self, term_df1, term_df2):
        droplist = []
        for index, row in term_df1.iteritems():
            if index in term_df2.columns:
                droplist.append(index)

        result = term_df1.drop(columns=droplist)
        return result

    def getNewXorResult(self, term1, term2):
        picklist = defaultdict(dict)
        if len(term1) < len(term2):
            for inx in term1.keys():
                if inx not in term2:
                    picklist[inx] = term1[inx]
            return picklist
        else:
            for inx in term2.keys():
                if inx not in term1:
                    picklist[inx] = term2[inx]
            return picklist

    def getORResult(self, term_df1, term_df2):
        _left_ = self.getXORResult(term_df1, term_df2)
        _right_ = self.getXORResult(term_df2, term_df1)
        _middle_ = self.getANDResult(term_df1, term_df2)
        subresult = pd.merge(_left_, _middle_, left_index=True, right_index=True)
        result = pd.merge(subresult, _right_, left_index=True, right_index=True)
        return result

    def getNewOrResult(self, term1, term2):
        term3 = defaultdict(dict)
        if len(term1) < len(term2):
            for inx in term1.keys():
                if inx in term2:
                    term3[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
                    term3[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
                else:
                    term3[inx]['pos'] = term1[inx]['pos']
                    term3[inx]['score'] = term1[inx]['score']
            return term3
        else:
            for inx in term2.keys():
                if inx in term1:
                    term3[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
                    term3[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
                else:
                    term3[inx]['pos'] = term2[inx]['pos']
                    term3[inx]['score'] = term2[inx]['score']
            return term3

    def getNewDisResult(self, term1, term2, distance):
        distance = int(distance)
        picklist = defaultdict(dict)
        if len(term1) < len(term2):
            for inx in term1.keys():
                if inx in term2:
                    for pos in term2[inx]['pos']:
                        pos = pos
                        dis = 0 if pos - distance < 0 else pos - distance
                        for i in range(dis, pos + distance + 1):
                            if i in term1[inx]['pos']:
                                picklist[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
                                picklist[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
            return picklist
        else:
            for inx in term2.keys():
                if inx in term1:
                    for pos in term1[inx]['pos']:
                        dis = 0 if pos - distance < 0 else pos - distance
                        for i in range(dis, pos + distance + 1):
                            if i in term2[inx]['pos']:
                                picklist[inx]['pos'] = term1[inx]['pos'] + term2[inx]['pos']
                                picklist[inx]['score'] = term1[inx]['score'] + term2[inx]['score']
            return picklist

    def getDisResult(self, term_df1, term_df2, distance):
        distance = int(distance)
        subresult = self.getANDResult(term_df1, term_df2)
        result = pd.DataFrame().reindex_like(term_df1)
        picklist = []
        for index, row in subresult.iteritems():
            for i in range(len(row["pos"]) - 2):
                if row["pos"][i + 1] - row["pos"][i] <= distance and (
                        ((row["pos"][i + 1] in term_df2[index]["pos"]) and (
                                row["pos"][i] in term_df1[index]["pos"]
                        )
                        ) or (
                                (row["pos"][i + 1] in term_df1[index]["pos"]) and (
                                row["pos"][i] in term_df2[index]["pos"]
                        )
                        )
                ):
                    if index not in picklist:
                        picklist.append(index)
            result = subresult[picklist]
        return result

    def finalize(self, result, mode='score'):
        """
        result : type dataframe 格式爲：
                            docid1     docid2     docid100
                    pos         []     []           []
                    score     Double   Double      Double

        return : 返回格式爲 list [ , , , , , ] 是排好順序的dict, 每個dict 的內容爲：
             {
                videoid: "",
                commentid: "",
                commentConetent : "",
                videoTitle: "",
                like: "",
                score: "",
             }
        """
        __result = result.sort_values(axis=1, by='score')
        deliver = []
        sqllist = []
        for id in __result.columns:
            commentid = id
            print(commentid)
            sql = "select C.videoid,C.id,C.comment_text,videotitle,likecount from eoogle.comment C, eoogle.video V where C.videoid = V.videoid and C.id = '" + commentid + "'"
            sqllist.append(sql)
            deliver.append(self.readFromMysql(sql))

        return deliver, sqllist

    def newFinalize(self, result, connector, mode='score'):
        """
        result : type dataframe 格式爲：
                            docid1     docid2     docid100
                    pos         []     []           []
                    score     Double   Double      Double

        return : 返回格式爲 list [ , , , , , ] 是排好順序的dict, 每個dict 的內容爲：
             {
                videoid: "",
                commentid: "",
                commentConetent : "",
                videoTitle: "",
                like: "",
                score: "",
             }
        """
        __result = sorted(result.items(), key=lambda item: item[1]['score'], reverse=True)
        deliver = []
        sql_comment_id = ''

        for id in __result:
            sql_comment_id += "'" + str(id[0]) + "'"
            sql_comment_id += ', '
        if len(sql_comment_id) > 0:
            sql_comment_id = sql_comment_id[:-2]
            sql = "select C.videoid,C.id,C.comment_text,videotitle,likecount from eoogle.comment C, eoogle.video V where C.videoid = V.videoid and C.id IN( " + sql_comment_id + ")"
            deliver.append(self.readFromMysql(sql, connector))
        # print(sql)
        #self.closeConnected()
        return deliver[0]

    def sortByLikeCount(self, deliver):
        return deliver.sort(reverse=True, key=lambda k: (k.get('likecount', 0)))


if __name__ == "__main__":
    start = time.time()
    searchservice = SearchHandle()
    mid1 = time.time()
    print("the init time is :", mid1 - start)

    connector1 = SqlCreator()
    #connector2 = SqlCreator()

    start2 = time.time()
    put = searchservice.initTerm("wait", connector1.getConn())
    get = searchservice.initTerm("jame", connector1.getConn())
    mid2 = time.time()
    print("time of finding data from db:", mid2 - start2)
    # distance search
    start3 = time.time()
    Dis_searchresult = searchservice.getNewDisResult(put, get, 100)
    mid3 = time.time()
    print("the dis search time is: ", mid3 - start3)
    # And search()
    start4 = time.time()
    and_searchresult = searchservice.getNewAndResult(put, get)
    mid4 = time.time()
    print("the AND search time is: ", mid4 - start4)
    # Or search()
    start5 = time.time()
    or_searchresult = searchservice.getNewOrResult(put, get)
    mid5 = time.time()
    print("the or search time is: ", mid5 - start5)
    # Xor search()
    start6 = time.time()
    xor_searchresult = searchservice.getNewXorResult(put, get)
    mid6 = time.time()
    print("the xor search time is: ", mid6 - start6)
    # Nei search()
    start7 = time.time()
    nei_searchresult = searchservice.getNewNeiResult(put, get)
    mid7 = time.time()
    print("the nei search time is: ", mid7 - start7)

    # full exmaple:
    start8 = time.time()
    connector1 = SqlCreator()
    searchservice = SearchHandle()
    put = searchservice.initTerm("wait", connector1.getConn())
    get = searchservice.initTerm("jame", connector1.getConn())
    example_or_search = searchservice.getNewOrResult(put, get)
    example_searchresult = searchservice.newFinalize(example_or_search, connector1.getConn())
    print("the example search time is ", time.time() - start8)
    # print(example_searchresult)
