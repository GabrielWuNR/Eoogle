import re
import os
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer

class preprocess():
    #
    def __init__(self):
        self.path = os.getcwd()

        ##两种stemmer结果一致，时间性能相近
        self.stemmer = SnowballStemmer('english')
        # self.stemmer = PorterStemmer()

        self.stopWordList = stopwords.words('english')
        self.stopDic = {}
        for word in self.stopWordList:
            self.stopDic[word] = 1

    def cut_pun_lower_case_remov_stop_get_stem(self, raw_text):
        line_cutpun = re.sub(r'[^\w\s]', ' ', raw_text)
        low_line_cutpun = [x.lower() for x in line_cutpun.split()]

        remov_stop = [word for word in low_line_cutpun if word not in self.stopDic]

        #通过profile测时间发现，时间主要消耗在stem上。但是stem词干提取不能省略，所以我只能优化到不去提取重复词的词干。
        #用字典存之前提取过词干的词，如果之前提取过这个词的词干，我不再重复提取之。
        # for word in remov_stop:
        #     if word in stem_dict:
        #         word_stem.append(stem_dict[word])
        #     else:
        #         word_aft_stem = self.stemmer.stem(word)
        #         word_stem.append(word_aft_stem)
        #         stem_dict[word] = word_aft_stem

        # 比较原来的直接提取词干，在3M的文件下时间从5秒优化到3秒
        # word_stem = [self.stemmer.stem(word) for word in remov_stop]

        return remov_stop


