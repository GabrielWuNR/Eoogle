import google.oauth2.credentials
import pymysql
import os
import pickle
import csv
import google.oauth2.credentials
import googleapiclient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# CLIENT_SECRETS_FILE = "client_secret_544508090807-gb0ris16m711fnb0n4qgq5aqq1qkeabv.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
# PI_SERVICE_NAME = 'youtube'
# API_VERSION = 'v3'
# DEVELOPER_KEY = "AIzaSyD8_pDy2L_oV40zNe2UbU5tERzkj_OLk0o"
DEVELOPER_KEY = "AIzaSyAGDvhlrD4o7XNCeKuJeF6_HFVgnGOQCZk"

class Handler2sql(object):

    def __init__(self, key, scope, dbname):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        self.key = key
        self.service = build('youtube', 'v3', developerKey=key)
        self.scope = scope
        self.dbname = dbname
        self.connent = pymysql.connect(host='localhost', user='iloveyuke', passwd='', db=dbname, charset='utf8')
        print("Initilized successfully")


    def get_video_comments(self, service, **kwargs):
        comments = []
        results = service.commentThreads().list(**kwargs).execute()
        i = 0
        max_page = 1

        while results and i < max_page:
            for item in results['items']:
                comment_temp = []
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_id = item['snippet']['topLevelComment']['id']
                comment_likecount = item['snippet']['topLevelComment']['snippet']['likeCount']
                comment_publishedtime = item['snippet']['topLevelComment']['snippet']['publishedAt']
                comment_updatedat = item['snippet']['topLevelComment']['snippet']['updatedAt']
                comment_temp.append(comment)  # 0
                comment_temp.append(comment_id)  # 1
                comment_temp.append(comment_likecount)  # 2
                comment_temp.append(comment_publishedtime)  # 3
                comment_temp.append(comment_updatedat)  # 4
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
    #             comments_writer.writerow(list(row))

    def search_videos_by_id(self, service, response):
        final_result = []
        video_by_id_comments = []
        video_tags = []
        video_category = []
        # id_list = ['28QYy8lrww8', 'AqtooBbxuaw', 'eYSmNP3woow', 'KiGsZACs9n4', 'XQ-iQreCkHc']
        for item in response['items']:
            video_id = item['id']
            # if video_id in id_list:
            #     continue
            video_title = item['snippet']['title']

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
            final_result.extend([(video_id, video_title, video_tags_str, video_category, comment[0], comment[1],
                                  comment[2], comment[3], comment[4]) for comment in video_by_id_comments])

        # for item in final_result:
        #     for i in item :
        #         print(i)
        #         print("\n")

        return final_result
        # write_to_csv(final_result)




    def pullMostPopulur(self, numberOfVideo):
        sql = ''
        response = self.service.videos().list(part="snippet", chart="mostPopular", locale="GB",
                                              maxResults=numberOfVideo).execute()
        result_list = self.search_videos_by_id(self.service, response)

        cur = self.connent.cursor()
        for item in result_list:
            print(item)
            for i in item:
                print(i)
                print("\n")
            sql = "INSERT INTO rawData_tbl VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}');".format(item[0],
                                                                                                              item[1],
                                                                                                              item[2],
                                                                                                              item[3],
                                                                                                              item[4],
                                                                                                              item[5],
                                                                                                              item[6],
                                                                                                              item[7],
                                                                                                              item[8])
            try:
                # Execute the SQL command
                cur.execute(sql)
                # Commit your changes in the database
                self.connent.commit()

            except:
                # Rollback in case there is any error
                print("Report error\n")
                self.connent.rollback()


if __name__ == '__main__':
    test = Handler2sql(DEVELOPER_KEY, SCOPES, 'PRODUCTS')
    test.pullMostPopulur(3)

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
