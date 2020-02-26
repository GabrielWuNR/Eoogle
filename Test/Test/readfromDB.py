import json
import os
import pymysql

class handlerwithsql(object):

    def __init__(self):
        self.read_count = 0
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        # connect to mysql
        self.connect = pymysql.connect(host='databasetry.c98rtvjmqwke.eu-west-2.rds.amazonaws.com', port=3306,
                                       user='admin', passwd='12345678', db='eoogle', charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
        print("Initilized successfully")

    def read2dict(self,sql):
        cur = self.connect.cursor()
        #sql = 'SELECT id, comment_text FROM comment'
        try:
            # Execute the SQL command
            self.read_count = cur.execute(sql)
            read_result = cur.fetchall()
           # print(read_result)
            return read_result

        except:
            # Rollback in case there is any error
            print("Read error\n")
            self.connect.rollback()



    def search4video(self):
        video_id = []
        cur = self.connect.cursor()

        #Search content can be gained from keyboard or a whole list
        #search_content = input('输入需要搜索的comment_Id：')
        #Transform the input list into a tuple string then batch search

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



if __name__ == '__main__':
    #SQL1='SELECT id, comment_text FROM comment'
    SQL1 = 'SELECT  videotitle FROM eoogle.video V;'
    test = handlerwithsql()
    readdict=test.read2dict(SQL1)
    print(readdict)
    readjson = json.dumps(readdict, indent=1)
    #print(readjson)
    videoid = test.search4video()
    with open("likecountorder.json", 'w') as f:
        f.write(readjson)
        # ignore it  just use readdict

