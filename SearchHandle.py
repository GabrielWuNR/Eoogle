import pandas as pd
import readfromDB
import fuzzysearch
import json
import pymongo
#from pymongo import MongoClient
from nltk.stem.porter import *
import pymysql
import os
import readfromDynamo
import time

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


class QueryError(Exception):
    def __init__(self, err='QueryError'):
        Exception.__init__(self, err)


class SearchHandle(object):
    def __init__(self):
        """
        需要在這裏分別接入 mysql 和 dymanoDB 的數據庫 然後再進行操作
        """
        self.stemer = PorterStemmer()
        self.DynamoDBService = readfromDynamo.DynamoDBService()
        self.sqlhandle = readfromDB.handlerwithsql()
        self.fuzzy = fuzzysearch.Fuzzy()

        print("Initilized successfully")

    def readFromMysql(self, sql):
        comment_dict = self.sqlhandle.read2dict(sql)
        self.total_comment = self.sqlhandle.read_count
        # self.sqlhandle.close_session()
        return comment_dict

    def readFromNosql(self, term):
        """
        term : 需要查的term
        result  : dict 包含該term的所有信息
        """
        __term = [term]
        terminfo = self.DynamoDBService.operate_table(table_name="BM25", terms=__term)
        return terminfo[term]

    def initTerm(self, term):
        """
        term : 在組合之前需要初始化的term
        return : term 對應的DataFrame
        """
        result = self.readFromNosql(term)
        if not bool(result):
            try:
                result = self.readFromNosql(self.fuzzy.bktreeSearch(term)[0][1])
            except IndexError:
                raise QueryError('QueryError')
        if not bool(result):
            try:
                result = self.readFromNosql(self.fuzzy.bktreeSearch(term)[1][1])
            except IndexError:
                raise QueryError('QueryError')
        if not bool(result):
            raise QueryError('QueryError')

        return pd.DataFrame.from_dict(result)

    def getOneResult(self, term_df):
        return term_df

    def getANDResult(self, term_df1, term_df2):
        result = (term_df1 + term_df2).dropna(axis=1)
        for index, row in result.iteritems():
            row['pos'].sort()
        return result

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
            #print()
        return result

    def getXORResult(self, term_df1, term_df2):
        droplist = []
        for index, row in term_df1.iteritems():
            if index in term_df2.columns:
                droplist.append(index)

        result = term_df1.drop(columns=droplist)
        return result

    def getORResult(self, term_df1, term_df2):
        _left_ = self.getXORResult(term_df1, term_df2)
        _right_ = self.getXORResult(term_df2, term_df1)
        _middle_ = self.getANDResult(term_df1, term_df2)
        subresult = pd.merge(_left_, _middle_, left_index=True, right_index=True)
        result = pd.merge(subresult, _right_, left_index=True, right_index=True)
        return result

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
        for id in __result.columns:
            commentid = id
            print(commentid)
            sql = "select C.videoid,C.id,C.comment_text,videotitle,likecount from eoogle.comment C, eoogle.video V where C.videoid = V.videoid and C.id = '" + commentid + "'"
            deliver.append(self.readFromMysql(sql))

        return deliver


if __name__ == "__main__":
    start = time.time()
    searchservice = SearchHandle()
    mid1 = time.time()
    print("the init time is :", mid1 - start)

    start3 = time.time()
    put = searchservice.initTerm("put")
    get = searchservice.initTerm("get")
    pytohn = searchservice.initTerm("pytohn")
    mid3 = time.time()
    print("time of finding data from db:", mid3 - start3)

    start2 = time.time()
    searchresult = searchservice.getANDResult(put, get)
    mid2 = time.time()
    print("the search time is: ", mid2 - start2)
    print(searchservice.finalize(searchresult))

    # print(searchservice.finalize(searchresult))
