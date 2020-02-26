# pip install pybktree
# pip install fuzzywuzzy
import collections
import json
import pybktree_mo
import time
import pymysql
import readfromDB

class Fuzzy():
    def __init__(self):
        with open('words_dictionary.json', 'r') as f:
            self.dictionary = json.load(f).keys()
            self.tree = pybktree_mo.BKTree(pybktree_mo.hamming_distance, self.dictionary)
             self.read_DB = readfromDB.handlerwithsql()

    def bktreeSearch(self, query):
        if len(query) <= 4:
            limit = 1
        else:
            limit = 2
        return sorted(self.tree.find(query, limit))
    
    def read_from_DB(self):
        Term_list=[]
        sql = 'SELECT Term FROM Term'
        Term_dict = self.read_DB.read2dict(sql)
        self.total_comment = self.read_DB.read_count
        self.read_DB.close_session()
        for item in Term_dict:
            Term_list.append(item['Term'])
        return Term_list
        


if __name__ == "__main__":
    time_1 = time.time()
    k = Fuzzy()
    time_2 = time.time()
    print("indexing time: ", time_2 - time_1)
    k.bktreeSearch('motherfucker')
    time_3 = time.time()
    print("Searching time:", time_3 - time_2)
     start = time()
    readterm=readterm()
    readterm.read_from_DB()
    stop = time()
    print(str(stop - start) + "s for generate term")

