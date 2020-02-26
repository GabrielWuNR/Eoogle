# pip install pybktree
# pip install fuzzywuzzy
import collections
import sys
sys.path.append('/opt/python/current/app/Test/')
import json
from Test import pybktree_mo
import time
import os

os.path.join(os.path.dirname(__file__), 'words_dictionary.json')
class Fuzzy():
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), 'words_dictionary.json'), 'r') as f:
            self.dictionary = json.load(f).keys()
            self.tree = pybktree_mo.BKTree(pybktree_mo.hamming_distance, self.dictionary)

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


