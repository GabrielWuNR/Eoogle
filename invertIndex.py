# This is the python file to generate the invert index
# 需要的库： nltk, collections, 确保invertIndex.py, preprocess.py, readfromDB.py在同一目录
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

    # get invert index dictionary of which the key is key words in getWordList()
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
                score = (float)(1 + math.log(tf, 10)) * math.log(self.total_comment / df, 10)
                tfidf_index[term][id] = score
        return tfidf_index

    def getInvertWithAvgdl(self):
        invert_index = defaultdict(dict)
        stem_dict = {}
        ld = defaultdict(dict)
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
                    ld[word][id] = len(word_preprocess)
                else:
                    invert_index[word][id].append(inx)
                    ld[word][id] = len(word_preprocess)
        return invert_index, ld

    def lucene(self, invert_index, ld, boost=1, k_1=1.2, b=0.75):
        """
        score(t,q,d) = Sigma_t^n( idf(t) * boost(t) * tfNorm(t,d) )
        idf(t): ln( 1 + (docCount-docFreq + 0.5)/(docFreq+0.5) )
            Where docCount: 文檔總數 docFreq: 含有文檔t 的數量
        boost(t): 查詢權重. 不指定時爲 1
        tfNorm(t, d): 使用BM25代替TF 算法:
            tfNorm(t, d) = f(t,d) * (k+1) / ( f(t,d) + k1*(1-b+b*(|D|/avgdl) ) )

        f(t,d) : 單詞t在文檔d中出現的次數
        k1: 詞語頻率飽和度，越低單詞數量影響越小，一般在1.2-2.0， 默認1.2
        b: 字段規約長度, 控制文本長度對結果的影響, 他的值在0-1直接之間，默認0.75
        |D| :文檔d中查詢該字段的文本長度
        avgdl: 文檔集合中，所有查詢該字段的平均長度

        """
        docCount = invert_index.length()
        lucene_index = defaultdict(dict)

        for term in invert_index:
            #        avgdl = sum([len(doc) for doc in ])
            docFreq = len(invert_index[term].keys())
            idf = math.log(1 + (docCount - docFreq + 0.5) / (docFreq + 0.5), 10)
            for i in ld[term]:
                avgdl = sum[ld[term]] / len[ld[term]]
            for id in invert_index[term]:
                tf = len(invert_index[term][id])
                tfNorm = tf * (k_1 + 1) / (tf + k_1 * (1 - b + b * (ld[term][id] / avgdl)))
                bmscore = idf * boost * tfNorm
                lucene_index[term][id] = bmscore
        return lucene_index


if __name__ == '__main__':
    # 处理20000条评论1.7s，生成tfidf 0.2s
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
