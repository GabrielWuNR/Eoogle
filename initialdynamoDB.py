import boto3
from boto3 import Session
from boto3.dynamodb.conditions import Attr, Key

class DynamoDBService:
    def __init__(self):
        self.this_day = datetime.date.today()
       
        self.AWS_ACCESS_ID = ''
        self.AWS_ACCESS_KEY = ''



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
    def operate_table(self, table_name="TFIDF"):
        # 通过dynamodb服务获取目标table的操作对象
        table_handle_h5_visit_info = self.get_service(table_name)
        """查询,根据某一key（column）查询"""
        response = table_handle_h5_visit_info.query(
            KeyConditionExpression=Key('uid').eq('')
        )
        print(response)
        # response中包含了很多内容，response本身是个json字符串，其Items键的内容才是table中的内容
        print( type(response))
        items = response['Items']
        print(items)
        print(json.dumps(items) )
