#This is the python file to generate the invert index
#需要的库： nltk, collections, 确保invertIndex.py, preprocess.py, readfromDB.py在同一目录
import os
import math
from preprocess import preprocess
from time import time
from collections import defaultdict
from nltk.stem import PorterStemmer
import readfromDB

class generateIndex():
    def __init__(self):
        self.path = os.getcwd()
        self.preprocess = preprocess()
        self.stemmer = PorterStemmer()
        self.read_DB = readfromDB.handlerwithsql()

    def read_from_DB(self):
        comment_dict = self.read_DB.read2dict()
        self.total_comment = self.read_DB.read_count
        self.read_DB.close_session()
        return comment_dict

    #get invert index dictionary of which the key is key words in getWordList()
    def getInvert(self):
        invert_index = defaultdict(dict)
        stem_dict = {}
        comment_list = self.read_from_DB()
        for items in comment_list:
            word_preprocess = []
            word_stem = []
            id = items['id']
            word_preprocess += self.preprocess.cut_pun_lower_case_remov_stop_get_stem(items['comment_text'])
            for word in word_preprocess:
                if word in stem_dict:
                    word_stem.append(stem_dict[word])
                else:
                    word_aft_stem = self.stemmer.stem(word)
                    word_stem.append(word_aft_stem)
                    stem_dict[word] = word_aft_stem
            for inx, word in enumerate(word_stem):
                if id not in invert_index[word]:
                    invert_index[word][id] = [inx]
                else:
                    invert_index[word][id].append(inx)
        return invert_index

    def tfidf(self, invert_index):
        tfidf_index = defaultdict(dict)
        for term in invert_index:
            df = len(invert_index[term].keys())
            for id in invert_index[term]:
                tf = len(invert_index[term][id])
                score = (float)(1+math.log(tf,10)) * math.log(self.total_comment / df, 10)
                tfidf_index[term][id] = score
        return tfidf_index


if __name__ == '__main__':
    #处理20000条评论1.7s，生成tfidf 0.2s
    initial = generateIndex()
    start = time()
    # invert 为反向索引，格式为dict{ term: dict{'id' : [pos1,pos2,pos3....]}}，只需要把这个存到mongoDB,其他逻辑不用动
    invert = initial.getInvert()
    stop = time()
    print(str(stop - start) + "s for invert index")
    start = time()
    # res 为tfidf， 格式为dict{term : dict {'id' : tfidf, 'id2' :tfidf....}}，
    res = initial.tfidf(invert)
    stop = time()
    print(str(stop - start) + "s for tfidf")

