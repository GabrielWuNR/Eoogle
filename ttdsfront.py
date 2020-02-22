import google.oauth2.credentials
import googleapiclient
import pymysql
import cryptography
import os

import pickle
import csv
import google.oauth2.credentials
import simplegoogleapi
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# CLIENT_SECRETS_FILE = "client_secret_544508090807-gb0ris16m711fnb0n4qgq5aqq1qkeabv.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
# PI_SERVICE_NAME = 'youtube'
# API_VERSION = 'v3'
DEVELOPER_KEY = "AIzaSyCk6K5GlgnV0Y5MJKbsPcG7E2H6KZO0G34"

MAIN_KEYS =["AIzaSyCT5uLLYlL63AIgnEbQdUiI3A3JLD0kml0","AIzaSyAhXGA3itr7HLvWhyGOomlG9mNjpGj6_HI","AIzaSyBTHOTEH7tNaw69dwQu14Ni94hERf4NWLI",
            "AIzaSyAa1RC9SwX1BTJRCmGZJWlz5zMfjQk9a0w","AIzaSyCrMSKNu9hjRR9WTvCA6AAKw6KLgU-UEf4","AIzaSyBHsOxi7FHAMf-PykF1uuBVVabRDW-afic",
            "AIzaSyCI5bGFBL7BUDzky0RHI9al4u4A3pheu38","AIzaSyBBCS7avE6NlJ-dlTbJ4wziXENW8QjMMUY","AIzaSyDEK47HzxiI2fYUjYYArCd1dEgPU2INcvI",
            "AIzaSyBzQrbPLx0bOXgp0MtTIuFbQPUkINgPiJc"]







class Handler2sql(object):

    def __init__(self, key, scope, dbname):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        self.key = key
        self.service = build('youtube', 'v3', developerKey=key)
        self.scope = scope
        self.dbname = dbname
        self.connent = pymysql.connect(host='databasetry.c98rtvjmqwke.eu-west-2.rds.amazonaws.com', user='admin', passwd='12345678', db='eoogle')
        print("Initilized successfully")


    def get_video_comments(self, service, **kwargs):
        comments = []
        results = service.commentThreads().list(**kwargs).execute()
        i = 0
        max_page = 200000
        cur = self.connent.cursor()
        while results and i < max_page:
            for item in results['items']:
                comment_temp = []
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                commento = item['snippet']['topLevelComment']['snippet']['textOriginal']
                comment_id = item['snippet']['topLevelComment']['id']
                comment_likecount = item['snippet']['topLevelComment']['snippet']['likeCount']
                comment_viewerRating = item['snippet']['topLevelComment']['snippet']['viewerRating']
                comment_publishedtime = item['snippet']['topLevelComment']['snippet']['publishedAt']
                comment_updatedat = item['snippet']['topLevelComment']['snippet']['updatedAt']
                comment_temp.append(comment_id)  # 0
                comment_temp.append(comment)  # 1
                comment_temp.append(commento)  # 2
                comment_temp.append(comment_likecount)  # 3
                comment_temp.append(comment_publishedtime)  # 4
                comment_temp.append(comment_updatedat)  # 5
                comments.append(comment_temp)

            if 'nextPageToken' in results:
                kwargs['pageToken'] = results['nextPageToken']
                results = service.commentThreads().list(**kwargs).execute()
                i+=1
            else:
                break
        return comments

    # def write_to_csv(self, comments):
    #     with open('comments_popular1.csv', 'w') as comments_file:
    #         comments_writer = csv.writer(comments_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #         comments_writer.writerow(['Video ID', 'Title', 'Video Tags', 'Category', 'Comment', 'CommentId', 'LikeCount', 'PublishedAt', 'UpdatedAt'])
    #         for row in comments:
    # comments_writer.writerow(list(row))
    def get_videos(self, service, response):
       video = {}
       for item in response:
           video_id = item['id']
           # if video_id in id_list:
           #     continue
           video_title = item['snippet']['title']
           # 添加
           try:
               video_tags = item['snippet']['tags']
               video_tags_str = " ".join(video_tags)
           except KeyError:
               video_tags_str = "NULL"
           if video_id not in video:
               print(video_title)
               video[video_id] = video_title
               sql = "INSERT INTO  video VALUES('{0}','{1}','{2}');".format(video_id,
                                                                            video[video_id].replace("'", "''"),
                                                                            video_tags_str.replace("'", "''"))
               cur = self.connent.cursor()
               try:
                   # Execute the SQL command
                   cur.execute(sql)
                   # Commit your changes in the database
                   self.connent.commit()
               except:
                   # Rollback in case there is any error
                   print("Report inout error  " + video_tags_str)
                   self.connent.rollback()
           try:
               video_category = item['snippet']['categoryId']
           except KeyError:
               pass
       return video

    def readCommentid(self):
        commentid = []
        cur = self.connect.cursor()
        comment_dict = {}
        sql = 'SELECT  id FROM comment'
        try:
            # Execute the SQL command
            cur.execute(sql)
            read_result = cur.fetchall()
            for items in read_result:
                commentid.append(items['id'])
        except:
            # Rollback in case there is any error
            print("Read error\n")
            self.connect.rollback()
        print(commentid)
        return commentid









    def search_videos_by_id(self, service, response):
        final_result = []
        video_by_id_comments = []
        video_tags = []
        video_category = []
        video={}
        try:
          commentid=self.readCommentid(self)
        except:
            print("read error of commentid")
        cur = self.connent.cursor()
        # id_list = ['28QYy8lrww8', 'AqtooBbxuaw', 'eYSmNP3woow', 'KiGsZACs9n4', 'XQ-iQreCkHc']
        for item in response:
            video_id = item['id']
            # if video_id in id_list:
            #     continue
            video_title = item['snippet']['title']
  # insert
            try:
                video_tags = item['snippet']['tags']
                video_tags_str = " ".join(video_tags)
            except KeyError:
                video_tags_str = "NULL"
            try:
                video_category = item['snippet']['categoryId']
            except KeyError:
                pass
            try:
                video_by_id_comments = self.get_video_comments(self.service, part='snippet', textFormat='plainText',
                                                               videoId=video_id)
            except googleapiclient.errors.HttpError:
                pass
            '''
            final_result.extend([(comment[0],comment[1].replace("'","''"),
                                  comment[2].replace("'","''"),
                                   video_id, video_category, comment[3], comment[4].replace("T"," ").replace("Z","").replace(".000",""),
                                  comment[5].replace("T"," ").replace("Z","").replace(".000","")) for comment in video_by_id_comments])
            '''
            for comment in video_by_id_comments:
             sql = "INSERT INTO comment VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}');".format(
                                  comment[0],comment[1].replace("'","''"),
                                  comment[2].replace("'","''"),
                                   video_id, video_category,
                                   comment[3],
                                   comment[4].replace("T"," ").replace("Z","").replace(".000",""),
                                   comment[5].replace("T"," ").replace("Z","").replace(".000",""))

             try:
                # Execute the SQL command
                cur.execute(sql)
                print("succeed!")
                # Commit your changes in the database
                self.connent.commit()

             except:
                # Rollback in case there is any error
                print("Report error\n")
                print(item)
                self.connent.rollback()


        # for item in final_result:
        #     for i in item :
        #         print(i)
        #         print("\n")

        return final_result
        # write_to_csv(final_result)




    def pullMostPopulur(self, response):
        sql = ''

        # response = self.service.videos().list(part="snippet", chart="mostPopular", locale="GB",
        #                                   maxResults=numberOfVideo).execute()
        result_list = self.search_videos_by_id(self.service, response)
        cur = self.connent.cursor()
        aaa=0
        for item in result_list:
            aaa=aaa+1
           # print(item)
            #for i in item:
                #print(i)
                #print("\n")
            '''
            sql = "INSERT INTO comment VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}');".format(item[0],
                                                                                                              item[1],
                                                                                                              item[2],
                                                                                                              item[3],
                                                                                                              item[4],
                                                                                                              item[5],
                                                                                                              item[6],
                                                                                                              item[7])

            try:
                # Execute the SQL command
                cur.execute(sql)
                # Commit your changes in the database
                self.connent.commit()

            except:
                # Rollback in case there is any error
                print("Report error\n")
                print(item)
                self.connent.rollback()
        print(aaa)
            
            
            '''

    def pullMostPopularReponse(self, numberOfVideo):
        response = self.service.videos().list(part="snippet", chart="mostPopular", locale="GB",
                                              maxResults=numberOfVideo).execute()
        return response




if __name__ == '__main__':
    test = Handler2sql(DEVELOPER_KEY, SCOPES, 'Eoogle')
    response = test.pullMostPopularReponse(30)
    responses = response['items']
    len_responses = len(responses)
    for i in range(10):
        start = 3 * i
        if start > len_responses - 1:
            pass
        end = start + 3
        if end > len_responses:
            end = len_responses
        sub_response = responses[start:end]
        now_key = MAIN_KEYS[i]
        sub_test = Handler2sql(now_key, SCOPES, 'Eoogle')
        sub_test.pullMostPopulur(sub_response)




    # When running locally, disable OAuthlib's HTTPs verification. When
    # running in production *do not* leave this option enabled.
    # os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    # service = get_authenticated_service()
    # response = service.videos().list(part="snippet", chart="mostPopular", locale="GB", maxResults=1).execute()
    # # id_list = []
    # # for one in response['items']:
    # #     id_list.append(one['id'])
    #
    # search_videos_by_id(service, response)
