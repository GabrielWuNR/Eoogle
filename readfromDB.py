import json

import pymysql
import os
import ast


class handlerwithsql(object):

    def __init__(self):
        self.read_count = 0

        # connect to mysql
        self.connect = pymysql.connect(host='databasetry.c98rtvjmqwke.eu-west-2.rds.amazonaws.com', port=3306,
                                       user='admin', passwd='12345678', db='eoogle', charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
        print("Initilized successfully")

    def read2dict(self, sql):
        cur = self.connect.cursor()
        # sql = 'SELECT id, comment_text FROM comment'
        try:
            # Execute the SQL command
            self.read_count = cur.execute(sql)
            # print("connection:" + self.connect.ping() + "-count:" + self.read_count)

            read_result = cur.fetchall()
            cur.close()
            # print(read_result)
            return read_result

        except:
            # Rollback in case there is any error
            # print("Read error\n"+ cur.Error)
            self.connect.rollback()

    def search4video(self):
        video_id = []
        cur = self.connect.cursor()

        # Search content can be gained from keyboard or a whole list
        # search_content = input('输入需要搜索的comment_Id：')
        # Transform the input list into a tuple string then batch search

        sql = 'SELECT videoid FROM video '
        try:
            # Execute the SQL command
            search_count = cur.execute(sql)
            search_result = cur.fetchall()
            print("The total number of search result is " + str(search_count))
            for items in search_result:
                video_id.append(items['videoid'])
            print('Corresponding video ids are ' + str(video_id))

        except:
            # Rollback in case there is any error
            print("Search error\n")
            self.connect.rollback()

        return video_id

    def close_session(self):
        self.connect.close()

    def readTerm25(self, terms):
        BM25 = {}
        for term in terms:
            BM25[term] = {}
            sql = "select * from eoogle.BM25 BM25 where Term ='" + term + "'"
            BM25_dict = self.read2dict(sql)
            print(BM25_dict)

            for item in BM25_dict:
                commentID = item['commentID']
                BM25[term][commentID] = {}

                if item['posID'][-1] != ']':
                    BM25[term][commentID]['pos'] = ast.literal_eval(item['posID'][:item['posID'].rfind(',')] + ']')
                    print()
                else:
                    BM25[term][commentID]['pos'] = ast.literal_eval(item['posID'])
                BM25[term][commentID]['score'] = float(item['score'])
        print(BM25)
        return BM25


if __name__ == '__main__':
    # SQL1='SELECT id, comment_text FROM comment'
    SQL1 = 'SELECT  videotitle FROM eoogle.video V;'
    test = handlerwithsql()
    # readdict=test.read2dict(SQL1)
    # print(readdict)
    # readjson = json.dumps(readdict, indent=1)
    # #print(readjson)
    # videoid = test.search4video()
    # with open("likecountorder.json", 'w') as f:
    #     f.write(readjson)
    #     # ignore it  just use readdict
    test.readTerm25(['2020'])
