import datetime
import decimal
import json
import os
import math
import boto3
from boto3 import Session
from collections import defaultdict
from decimal import Decimal
import concurrent.futures
import time

from botocore.exceptions import ClientError

import invertIndex2
from boto3.dynamodb.conditions import Attr, Key

import initialdynamoDB
DynamoDBService = initialdynamoDB.DynamoDBService()
table = DynamoDBService.get_service(table_name="TFIDF")

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# class DynamoDBService:
#     def __init__(self):
#         self.this_day = datetime.date.today()
#         # 这两个key像是账号和密码一般，需要在后台申请导出，唯一的
#         self.AWS_ACCESS_ID = 'AKIAIBXEM26CPJQRVUXA'
#         self.AWS_ACCESS_KEY = 'Y6YmzwHi8sBF8EmWSpXJqP9H3XOCAkpvtaltZXYz'
#
#
#
#     def get_service(self, table_name):
#         """将service单独拿出来的目的，我为了初始化类的时候不会那么慢"""
#         client = boto3.client('dynamodb', region_name='eu-west-2',
#                               aws_access_key_id=self.AWS_ACCESS_ID,
#                               aws_secret_access_key=self.AWS_ACCESS_KEY)
#         dynamodb = boto3.resource('dynamodb', region_name='eu-west-2',
#                                   aws_access_key_id=self.AWS_ACCESS_ID,
#                                   aws_secret_access_key=self.AWS_ACCESS_KEY)
#         # 通过dynamodb服务获取目标table的操作对象
#         table_handle = dynamodb.Table(table_name)
#         print("Table status:", table_handle.table_status)
#         return table_handle
#
#
#     #table_handle_h5_visit_info = self.get_service(table_name)
#     def operate_table(self, table_name="TFIDF"):
#         # 通过dynamodb服务获取目标table的操作对象
#         table_handle_h5_visit_info = self.get_service(table_name)
#         """查询,根据某一key（column）查询"""
#         response = table_handle_h5_visit_info.query(
#             KeyConditionExpression=Key('uid').eq('f3d61094c65a42489d0e54d4c30b7e6f')
#         )
#         print(response)
#         # response中包含了很多内容，response本身是个json字符串，其Items键的内容才是table中的内容
#         print( type(response))
#         items = response['Items']
#         print(items)
#         print(json.dumps(items) )

def processTFIDF(item):
    Term = str(item['term'])
    CommentID = str(item['CommentID'])
    posID = item['CommentInfo']['posID']
    score = item['CommentInfo']['score']
    # print("Adding TFIDF:", Term, CommentID, posID,score)
    try:
        response_get = table.get_item(
            Key={
                'Term': Term,
                'CommentID': CommentID})
        item_read = response_get['Item']
        if item_read:
            try:
                response_update = table.update_item(
                    Key={
                        'Term': Term,
                        'CommentID': CommentID
                    },
                    UpdateExpression="set score = :r, posID=:a",
                    ExpressionAttributeValues={
                        ':r': score,
                        ':a': posID
                    },
                    ReturnValues="UPDATED_NEW"
                )
                print("TFIDF record update SUCCEED!!")
            except:
                print("TFIDF record update failed!")
        else:
            print(item_read)
            print("Not in TFIDF table")
            try:
                table.put_item(
                    Item={
                        'Term': Term,
                        'CommentID': CommentID,
                        'score': score,
                        'posID': posID}
                )
                print("new TFIDF record writing succeed！")
            except:
                print("input error")


    except:
        print("Read error")
        print(item_read)
        print("Not in TFIDF table")
        try:
            table.put_item(
                Item={
                    'Term': Term,
                    'CommentID': CommentID,
                    'score': score,
                    'posID': posID}
            )
            print("new TFIDF record writing succeed！")
        except:
            print("input error")


def main():

    initial = invertIndex2.generateIndex()
    invert = initial.getInvert()
    res = initial.tfidf(invert)
    TFIDF = initial.change_format(res)
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(processTFIDF, TFIDF)
    finish = time.time()
    print('%d second' % (finish - start_time))


if __name__ == '__main__':
    main()

    # DynamoDBService=DynamoDBService()
    # table=DynamoDBService.get_service(table_name = "TFIDF")









   

