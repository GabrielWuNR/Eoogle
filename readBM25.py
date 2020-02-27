import json
import pymysql
import os
import ast
import time


class readfromMysqlBM25(object):

    def __init__(self):
        #os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        # connect to mysql
        self.connect = pymysql.connect(host='databasetry.c98rtvjmqwke.eu-west-2.rds.amazonaws.com', port=3306,
                                       user='admin', passwd='12345678', db='eoogle', charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
        print("Initilized successfully")

    def read2dict(self,sql):
        cur = self.connect.cursor()
        try:
            self.read_count = cur.execute(sql)
            read_result = cur.fetchall()
            cur.close()
            return read_result
        except:
            print("Read error\n")
            self.connect.rollback()

if __name__ == '__main__':
    #SQL1='SELECT id, comment_text FROM comment'
    '''
     start_time = time.time()
    terms=['wait','jame','moment']
    readTerm25(terms)
    end_time = time.time()
    print('%s second' % (end_time - start_time))
    
    '''











