import collections
from Test import SearchHandle
from Test import load_stop
from time import time
from nltk.stem import PorterStemmer
#from nltk.corpus import stopwords
import os
import re
import json

class parse_search():
    def __init__(self):
        """
        初始化数据库搜索模块
        """
        load_stop.loadStop().load_json()
        self.stemmer = PorterStemmer()
        self.search = SearchHandle.SearchHandle()
        self.stopDic = load_stop.loadStop().stopwordslist
        print("Initilized successfully")

    def preprocess_query(self, query):
        res = ''
        remov_stop = [word for word in query.split() if word not in self.stopDic]
        for word in remov_stop:
            res += word
            res += ' '
        return res.rstrip(' ')

    def preprocess_word(self, term):
        term_cutpun = re.sub(r'[^\w\s]', '', term)
        return self.stemmer.stem(term_cutpun.lower())

    def deal_with_phase(self, queue, qword, opt, sql_connector):
        term = ''
        phase_terms = []
        count_space = 0

        # 找第二个双引号，中间有空格无视掉
        while (len(queue) > 0 and queue[0] != '"'):
            if (queue[0] != ' '):
                term += queue.popleft()
            else:
                count_space += 1
                queue.popleft()
                if term != '':
                    phase_terms.append(self.preprocess_word(term))
                term = ''

        # 增加最后一个词
        if term != '':
            phase_terms.append(self.preprocess_word(term))

        # 如果不符合格式，没有出现第二个双引号，全以OR搜索处理
        if (len(queue) == 0):
            print("no words need to be phased，use all OR search")
            for word in phase_terms:
                try:
                    qword.append(self.search.initTerm(self.preprocess_word(word), sql_connector.getConn()))
                except SearchHandle.QueryError:
                    count_space -= 1
                    pass
            for i in range(0, count_space):
                opt.append('OR')

        # 如果符合格式，且遇见了双引号，去掉双引号，对双引号之间的词做邻居搜索
        if (len(queue) > 0 and queue[0] == '"'):
            queue.popleft()
            if (len(phase_terms) == 1):
                print("no words need to be phased")
                try:
                    qword.append(self.search.initTerm(phase_terms[0], sql_connector.getConn()))
                except SearchHandle.QueryError:
                    pass
            else:
                while len(phase_terms) > 0:
                    try:
                        res = self.search.initTerm(phase_terms[0], sql_connector.getConn())
                        break
                    except SearchHandle.QueryError:
                        del phase_terms[0]
                        pass
                while len(phase_terms) > 1:
                    del phase_terms[0]
                    try:
                        res = self.search.getNewNeiResult(res,
                                                          self.search.initTerm(phase_terms[0], sql_connector.getConn()))
                    except SearchHandle.QueryError:
                        pass
                qword.append(res)

    def deal_with_proximity(self, queue, qword, opt, sql_connector):
        term = ''
        distance = ''
        count_space = 0
        proximity_terms = []

        if (len(queue) == 0):
            print("just search a #\n")

        # 如果找不到左括号，判定用户正在输入数字，如果此时有空格判定无效输入直接跳出
        # 按原状态机方式进行搜索
        while (len(queue) > 0 and queue[0] != '('):
            if (queue[0] != ' '):
                distance += queue.popleft()
            else:
                break

        # 如果还没找到左括号就结束了，把这个词放进搜索list,返回平常方法
        if (len(queue) == 0 or queue[0] != '('):
            try:
                qword.append(self.search.initTerm(self.preprocess_word(distance), sql_connector.getConn()))
            except SearchHandle.QueryError:
                if (len(queue) > 0 and queue[0] == '('):
                    queue.popleft()
                pass
            return
        else:
            # 如果找到了左括号，将左括号剔除
            queue.popleft()

        # 进入距离搜索阶段，找右括号
        while (len(queue) > 0 and queue[0] != ')'):
            if (queue[0] == ','):
                if term != '':
                    proximity_terms.append(self.preprocess_word(term))
                term = ''
                while (queue[0] == ','):
                    count_space += 1
                    queue.popleft()
            if (queue[0] == ' '):
                queue.popleft()
            else:
                term += queue.popleft()
        proximity_terms.append(self.preprocess_word(term))
        term = ''
        if (len(queue) == 0):
            for word in proximity_terms:
                try:
                    qword.append(self.search.initTerm(word, sql_connector.getConn()))
                except SearchHandle.QueryError:
                    count_space -= 1
                    pass
            for i in range(0, count_space):
                opt.append('OR')
        else:
            queue.popleft()
            while len(proximity_terms) > 0:
                try:
                    self.search.initTerm(proximity_terms[0], sql_connector.getConn())
                    break
                except SearchHandle.QueryError:
                    del proximity_terms[0]
                    pass

            res = self.search.initTerm(proximity_terms[0], sql_connector.getConn())

            while len(proximity_terms) > 1:
                del proximity_terms[0]
                try:
                    res = self.search.getNewDisResult(res,
                                                      self.search.initTerm(proximity_terms[0], sql_connector.getConn()),
                                                      distance)
                except SearchHandle.QueryError:
                    pass
            qword.append(res)

    # judge the user input to get what type of search user want
    def getSearch(self, query):
        qword = []
        opt = []

        sql_connector = SearchHandle.SqlCreator()

        query = self.preprocess_query(query)

        queue = collections.deque(query)

        term = ''
        flag = 'none'
        while (len(queue) > 0):
            temp = queue.popleft()
            if temp == '#':
                self.deal_with_proximity(queue, qword, opt, sql_connector)
                flag = 'none'
            elif temp == '"':
                self.deal_with_phase(queue, qword, opt, sql_connector)
                flag = 'none'

            # 空格的辨识，如果没有逻辑运算符就按OR处理
            elif temp == ' ':
                if flag == 'none' and len(queue) > 0:
                    if term == 'AND':
                        opt.append('AND')
                        flag = 'AND'
                        term = ''
                    elif term == 'NOT' and flag == 'AND':
                        opt[-1] = 'AND NOT'
                        flag = 'NOT'
                        term = ''
                    elif term == 'NOT' and flag != 'AND':
                        flag = 'none'
                        qword.append(self.search.initTerm(self.preprocess_word(term), sql_connector.getConn()))
                        term = ''
                    elif term == 'OR':
                        opt.append('OR')
                        flag = 'OR'
                        term = ''
                    else:
                        # 如果此时没有逻辑状态自动补全OR
                        if flag == 'none':
                            if (len(queue) <= 3):
                                opt.append('OR')
                            elif len(queue) > 3 and (queue[0] + queue[1] + queue[2]) != 'AND' and (
                                    queue[0] + queue[1]) != 'OR':
                                opt.append('OR')
                        if term != '':
                            try:
                                qword.append(self.search.initTerm(self.preprocess_word(term), sql_connector.getConn()))
                            except SearchHandle.QueryError:
                                if flag == 'none' and len(opt) > 0:
                                    del opt[-1]
                                pass
                            flag = 'none'
                            term = ''
                else:
                    if term == 'NOT' and flag == 'AND':
                        opt[-1] = 'AND NOT'
                        flag = 'NOT'
                        term = ''

                    if term != '':
                        try:
                            qword.append(self.search.initTerm(self.preprocess_word(term), sql_connector.getConn()))
                        except SearchHandle.QueryError:
                            pass
                    term = ''

            # if we get a phrase search, we will store the result list in the
            # term queue
            else:
                if (temp != ' '):
                    term += temp

        if term != '':
            try:
                qword.append(self.search.initTerm(self.preprocess_word(term), sql_connector.getConn()))
            except SearchHandle.QueryError:
                if len(opt) > 0:
                    del opt[-1]
                pass
        if len(qword) > 0:
            res = qword[0]
        else:
            return []
        while len(opt) > 0 and len(qword) > 0:
            del qword[0]
            if opt[0] == 'AND NOT':
                print('in and not')
                res = self.search.getNewXorResult(res, qword[0])
            if opt[0] == 'AND':
                res = self.search.getNewAndResult(res, qword[0])
            if opt[0] == 'OR':
                print('in or')
                res = self.search.getNewOrResult(res, qword[0])
            del opt[0]

        try:
            res = self.search.newFinalize(res, sql_connector.getConn())
        except SearchHandle.QueryError:
            return []

        sql_connector.close()

        return res
