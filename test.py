# 172.19.16.1
import base64
import json

import requests

localhost = '172.19.16.1'
# #测试能否远程连接mysql数据库
# #
# import pymysql
# try:
#     conn = pymysql.connect(host='172.19.16.1', user='root', password='18921190757ytk', db='zion', charset='utf8')
#     print(conn)
#     print("连接成功")
# except Exception as e:
#     print(e)
#     print("连接失败")
#     #关闭连接
#     conn.close()
# #
# user = "admin"
# password = "Complexpass#123"
# query_content = "resigned"
# # 将用户名和密码进行 base64 编码
# bas64encoded_creds = base64.b64encode(bytes(user + ":" + password, "utf-8")).decode("utf-8")
# # 查询参数
# params = {
#     "search_type": "querystring",
#     "query": {
#         "term": query_content
#     },
#     "_source": ["sender", "reciver", "date", "content"]
# }
# # 设置请求头，包括认证信息
# headers = {
#     "Content-type": "application/json",
#     "Authorization": "Basic " + bas64encoded_creds
# }
# # 索引名称
# index = "email"
# # ZincSearch 主机地址
# zinc_host = f"http://{localhost}:4080"
# # 完整的搜索 URL
# zinc_url = zinc_host + "/api/" + index + "/_search"
#
# # 发送 POST 请求进行搜索
# res = requests.post(zinc_url, headers=headers, data=json.dumps(params))
# #打印状态=
# print(res.status_code)
# # 打印搜索结果
# print(res.json())


#测试连接milvus数据库
import time
import time as time1
import numpy as np
import requests
start_time = time1.time()
from datetime import datetime, time
import pandas as pd
import pymysql
from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer
localhost ='172.19.16.1'

connections.connect('default', host=f'{localhost}', port=19530)
print('connect success')
collection_name = 'email_embeddings'
collection = Collection(name=collection_name)
#测试是否连接成功
