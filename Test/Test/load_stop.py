import json
import sys
import os
sys.path.append('/opt/python/current/app/Test/')

class loadStop():
    def __init__(self):
        self.stopwordslist = {}

    def load_json(self):
        with open(os.path.join(os.path.dirname(__file__),'stopwords.json'), 'r') as fp:
            self.stopwordslist = json.load(fp)