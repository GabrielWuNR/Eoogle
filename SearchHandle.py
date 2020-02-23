import pandas as pd
import readfromDB
import fuzzysearch
import json
import pymongo
from pymongo import MongoClient
from nltk.stem.porter import *
import pymysql
import os

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


class SearchHandle(object):
    def __init__(self, dbname, host='localhost', port='27017'):
        """
        需要在這裏分別接入 mysql 和 dymanoDB 的數據庫 然後再進行操作
        """
        self.sqlhandle = readfromDB.handlerwithsql()
        self.fuzzy = fuzzysearch.Fuzzy()

        print("Initilized successfully")

    def readFromMysql(self, sql):
        comment_dict = self.sqlhandle.read2dict(sql)
       # self.total_comment = self.sqlhandle.read_count
        self.sqlhandle.close_session()
        return comment_dict

    def __readFromNosql(self, term):
        """
        term : 需要查的term
        result  : dict 包含該term的所有信息
        """
        term = self.stem.stem(term)
        result = self.collection.find_one({"term": term})
        return result

    def initTerm(self, term):
        """
        term : 在組合之前需要初始化的term
        return : term 對應的DataFrame
        """
        try:
            result = self.__readFromNosql(term)
        except :
            result = self.__readFromNosql(self.fuzzy.bktreeSearch(term)[1])
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
        result = pd.DataFrame().reindex_like(term_df1)
        for index, row in subresult.iteritems():
            droplist = []
            for i in range(len(row["pos"]) - 2):
                if row["pos"][i + 1] - row["pos"][i] == 1:
                    if not (row["pos"][i + 1] in term_df2[index]["pos"]) and (row["pos"][i] in term_df1[index]["pos"]):
                        droplist.append(index)
            result = subresult.drop(columns=droplist)
        return result

    def getXORResult(self, term_df1, term_df2):
        droplist = []
        for index, row in term_df1.iteritems():
            if index in term_df1.columns:
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
        subresult = self.getANDResult(term_df1, term_df2)
        result = pd.DataFrame().reindex_like(term_df1)
        for index, row in subresult.iteritems():
            droplist = []
            for i in range(len(row["pos"]) - 2):
                if row["pos"][i + 1] - row["pos"][i] <= distance:
                    if not ((row["pos"][i + 1] in term_df2[index]["pos"]) and (
                            row["pos"][i] in term_df1[index]["pos"])) or (
                            (row["pos"][i + 1] in term_df1[index]["pos"]) and (
                            row["pos"][i] in term_df2[index]["pos"])):
                        droplist.append(index)
            result = subresult.drop(columns=droplist)
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
        for index, row in __result.iteritems():
            commentid = index
            sql = "select C.videoid,C.id,C.comment_text,videotitle,likecount from eoogle.comment C, eoogle.video V where C.videoid = V.videoid and C.id = '" + commentid + "'"
            deliver.append(self.readFromMysql(sql))

        return deliver
