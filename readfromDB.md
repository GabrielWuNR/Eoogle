首先 import readfromDB

类中定义函数


def read_from_DB(self,sql):
    comment_dict = self.read_DB.read2dict(sql)
    self.total_comment = self.read_DB.read_count
    self.read_DB.close_session()
    return comment_dict
    
类中使用：
commentid=''
sql="select C.videoid,C.id,C.comment_text,videotitle,likecount 
from eoogle.comment C ,eoogle.video V where C.videoid=V.videoid and
 C.id="+commentid
 
 
self.read_from_DB(sql)