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
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
DEVELOPER_KEY = "AIzaSyAGDvhlrD4o7XNCeKuJeF6_HFVgnGOQCZk"


def get_authenticated_service():
    return build(API_SERVICE_NAME, API_VERSION, developerKey=DEVELOPER_KEY)


def get_video_comments(service, **kwargs):
    comments = []
    results = service.commentThreads().list(**kwargs).execute()

    while results:
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
        else:
            break

    return comments


def write_to_csv(comments):
    with open('comments_popular20.csv', 'w') as comments_file:
        comments_writer = csv.writer(comments_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        comments_writer.writerow(['Video ID', 'Title', 'Video Tags', 'Category', 'Comment', 'CommentId', 'LikeCount',
                                  'PublishedAt', 'UpdatedAt'])
        for row in comments:
            comments_writer.writerow(list(row))


def search_videos_by_id(service, response):

    comment = []
    final_result = []
    id_read_list = []
    # with open('id_list.txt', 'r') as lists:
    #     for single in lists:
    #         id_read_list.append(single.strip('\n'))
    # lists.close()
    for item in response['items']:
        video_id = item['id']
        print("This video id is [0]".format(video_id))
        # if video_id in id_read_list:
        #     continue
        video_title = item['snippet']['title']
        video_tags = []
        try:
            video_tags = item['snippet']['tags']
            video_tag_str = " ".join(video_tags)
        except KeyError:
            pass
        video_category = []
        try:
            video_category = item['snippet']['categoryId']
        except KeyError:
            pass
        video_by_id_comments = []
        try:
            video_by_id_comments = get_video_comments(service, part='snippet', maxResults=1, textFormat='plainText', videoId=video_id)
        except googleapiclient.errors.HttpError:
            pass
        final_result.extend([(video_id, video_title, video_tags_str, video_category, comment[0], comment[1],
                             comment[2], comment[3], comment[4]) for comment in video_by_id_comments])

    return final_result
   # write_to_csv(final_result)


if __name__ == '__main__':
    # When running locally, disable OAuthlib's HTTPs verification. When
    # running in production *do not* leave this option enabled.

    connect = pymysql.connect(host='localhost', user='iloveyuke', passwd='', db='PRODUCTS')
    cur = connect.cursor()
    print("connected to the sql serve")
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'


    service = get_authenticated_service()
    response = service.videos().list(part="snippet", chart="mostPopular", locale="GB", maxResults=3).execute()
    # id_list = []
    # for one in response['items']:
    #     id_list.append(one['id'])

    result = search_videos_by_id(service, response)
    allnum = len(result)
    print("the whole number of message is [0]".format(allnum))
    i = 0
    for item in result :
        sql = "INSERT RawData_tbl VALUES (`{0}`,`{1}`,`{2}`,`{3}`,`{4}`,`{5}`,`{6}`.`{7}`,`{8}`);".format(item[0],
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
            connent.commit()

        except:
            # Rollback in case there is any error
            print("Report error\n")
            connent.rollback()

        print("the process [0] \n".format(i/allnum))






