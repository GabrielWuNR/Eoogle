# pip install pybktree
# pip install fuzzywuzzy
import collections
import json
import pybktree_mo
import time
import readfromDB


class Fuzzy():
    def __init__(self):
        self.readhandle = readfromDB.handlerwithsql()
        Term_list=[]
        sql = 'SELECT Term FROM Term'
        Term_dict = self.readhandle.read2dict(sql)
        self.total_comment = self.readhandle.read_count
        #self.readhandle.close_session()
        for item in Term_dict:
            Term_list.append(item['Term'])
        
        self.tree = pybktree_mo.BKTree(pybktree_mo.hamming_distance, Term_list)
        
        
    def bktreeSearch(self, query):
        if len(query) <= 4:
            limit = 1
        else:
            limit = 2
        return sorted(self.tree.find(query, limit))





if __name__ == "__main__":
    time_1 = time.time()
    k = Fuzzy()
    time_2 = time.time()
    print("indexing time: ", time_2 - time_1)
    k.bktreeSearch('motherfucker')
    time_3 = time.time()
    print("Searching time:", time_3 - time_2)



