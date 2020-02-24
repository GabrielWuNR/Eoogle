import datetime
import json
import os
import math
import boto3
from boto3 import Session
from collections import defaultdict
from decimal import Decimal

from pandas import DataFrame
import invertIndex2

from boto3.dynamodb.conditions import Attr, Key
class DynamoDBService:
    def __init__(self):
        self.this_day = datetime.date.today()
        # 这两个key像是账号和密码一般，需要在后台申请导出，唯一的
        self.AWS_ACCESS_ID = 'AKIAIBXEM26CPJQRVUXA'
        self.AWS_ACCESS_KEY = 'Y6YmzwHi8sBF8EmWSpXJqP9H3XOCAkpvtaltZXYz'



    def get_service(self, table_name):
        """将service单独拿出来的目的，我为了初始化类的时候不会那么慢"""
        client = boto3.client('dynamodb', region_name='eu-west-2',
                              aws_access_key_id=self.AWS_ACCESS_ID,
                              aws_secret_access_key=self.AWS_ACCESS_KEY)
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-2',
                                  aws_access_key_id=self.AWS_ACCESS_ID,
                                  aws_secret_access_key=self.AWS_ACCESS_KEY)
        # 通过dynamodb服务获取目标table的操作对象
        table_handle = dynamodb.Table(table_name)
        print("Table status:", table_handle.table_status)
        return table_handle


    #table_handle_h5_visit_info = self.get_service(table_name)
    def operate_table(self,table_name,terms):
        termTFIDF={}

        # 通过dynamodb服务获取目标table的操作对象
        table_handle = self.get_service(table_name)
        """查询,根据某一key（column）查询"""
        for term in terms:
          print(term)
          termTFIDF[term] = {}
          response = table_handle.query( KeyConditionExpression=Key('Term').eq(term)   )
          #print(response)
          # print("\n\n\n\n")
          for item in response["Items"]:
              commentID = item['CommentID']
              posID = []
              for pos in item['posID']:
                  posID.append(int(pos))
              score = item['score']
              termTFIDF[term][commentID] = {}
              termTFIDF[term][commentID]['pos'] = posID
              termTFIDF[term][commentID]['score'] = float(score)
        #print(termTFIDF)
        return termTFIDF
        # response中包含了很多内容，response本身是个json字符串，其Items键的内容才是table中的内容





if __name__ == '__main__':
    term=['put','got','bomb']#示例   输入的term为list
    DynamoDBService = DynamoDBService()
    TFIDF_term = DynamoDBService.operate_table(table_name="TFIDF",terms=term)




