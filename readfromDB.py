import pymysql
import os

class handlerwithsql(object):

    def __init__(self):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        #connect to mysql
        self.connect = pymysql.connect(host='databasetry.c98rtvjmqwke.eu-west-2.rds.amazonaws.com', port = 3306,
                                       user='admin', passwd='12345678', db='eoogle', charset='utf8', cursorclass=pymysql.cursors.DictCursor)
        print("Initilized successfully")


    def read2dict(self):
        cur = self.connect.cursor()
        sql = 'SELECT id, comment_text FROM comment'
        try:
            # Execute the SQL command
            read_count = cur.execute(sql)
            read_result = cur.fetchall()
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
        search_content = tuple(['1','2','3'])
        sql = 'SELECT videoid FROM comment WHERE id in' + str(search_content)
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
    test = handlerwithsql()
    test.read2dict()
    test.search4video()
