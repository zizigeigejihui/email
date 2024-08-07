import base64
import json
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

model = SentenceTransformer('D:\\bge-large-en-v1.5')
connections.connect('default', host='localhost', port=19530)
collection_name = 'email_embeddings'
collection = Collection(name=collection_name)


def select1(sender, start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    start_date = start_date.replace(hour=0, minute=0, second=0)
    end_date = end_date.replace(hour=23, minute=59, second=59)
    start_date_formatted = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_date_formatted = end_date.strftime("%Y-%m-%d %H:%M:%S")
    params = (sender, sender, start_date_formatted, end_date_formatted)
    start_time = time1.time()
    conn = pymysql.connect(host='localhost', user='root', password='18921190757ytk', database='zion')
    params = (sender, sender, start_date_formatted, end_date_formatted)
    if sender!='':
        with conn.cursor() as cursor:
            sql_query = """
                SELECT * FROM parased_emails
                WHERE (sender = %s OR reciver = %s)
                AND date BETWEEN %s AND %s order by date
                """
            cursor.execute(sql_query, (sender, sender, start_date_formatted, end_date_formatted))
            result = cursor.fetchall()
        parased_emails_df = pd.DataFrame(result)
        parased_emails_df.columns = ['message_id', 'date', 'sender', 'reciver', 'content', 'index']
        email_id_list = parased_emails_df['index'].tolist()
    return email_id_list, parased_emails_df
def select2(sender, start_date_str, end_date_str, query_content):
    email_index_id, email_df = select1(sender, start_date_str, end_date_str)
    query_embedding = model.encode(query_content)
    filtered_id = [int(i) for i in email_index_id]
    search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
    top_k = 1000
    results = collection.search(
        data=[query_embedding],
        anns_field="embedding",
        param=search_params,
        limit=top_k,
        expr=f'email_id in {filtered_id}',
        output_fields=["email_id"]
    )
    result_ids = []
    distances = [hit.distance for result in results for hit in result]
    # 计算平均值和标准差
    mean_distance = np.mean(distances)
    std_distance = np.std(distances)
    # 使用平均值加上一个标准差作为阈值
    threshold_std = mean_distance + std_distance
    # 计算四分位数和 IQR
    Q1 = np.percentile(distances, 25)
    Q3 = np.percentile(distances, 75)
    IQR = Q3 - Q1
    # 使用 IQR 方法计算上限阈值
    threshold_iqr = Q3 + 1.5 * IQR
    # 可以根据需要选择一种方法作为阈值
    threshold = max(threshold_std, threshold_iqr)
    for result in results:
        for hit in result:
            if hit.distance > threshold:
                result_ids.append(hit.entity.get('email_id'))
    result_df = email_df.loc[email_df['index'].isin(result_ids)]
    return result_df
def select3(query_content):
    # 用户名和密码
    user = "admin"
    password = "Complexpass#123"
    # 将用户名和密码进行 base64 编码
    bas64encoded_creds = base64.b64encode(bytes(user + ":" + password, "utf-8")).decode("utf-8")
    # 查询参数
    params = {
        "search_type": "querystring",
        "query": {
            "term": query_content
        },
        "_source": ["sender", "reciver", "date", "content"]
    }
    # 设置请求头，包括认证信息
    headers = {
        "Content-type": "application/json",
        "Authorization": "Basic " + bas64encoded_creds
    }
    # 索引名称
    index = "email"
    # ZincSearch 主机地址
    zinc_host = "http://localhost:4080"
    # 完整的搜索 URL
    zinc_url = zinc_host + "/api/" + index + "/_search"

    # 发送 POST 请求进行搜索
    res = requests.post(zinc_url, headers=headers, data=json.dumps(params))

    # 检查响应状态码
    if res.status_code == 200:
        # 解析响应结果
        res_json = res.json()
        hits = res_json.get('hits', {}).get('hits', [])

        # 提取每个文档的_source字段
        data = [
            {
                "sender": hit["_source"].get("sender"),
                "reciver": hit["_source"].get("reciver"),
                "date": hit["_source"].get("date"),
                "content": hit["_source"].get("content"),
            }
            for hit in hits
        ]

        # 转换为 DataFrame
        df = pd.DataFrame(data)

        return df
    else:
        # 如果请求失败，返回空 DataFrame 并打印错误信息
        print(f"Failed to search: {res.text}")
        return pd.DataFrame()
def select4(sender, start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    start_date = start_date.replace(hour=0, minute=0, second=0)
    end_date = end_date.replace(hour=23, minute=59, second=59)
    start_date_formatted = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_date_formatted = end_date.strftime("%Y-%m-%d %H:%M:%S")
    params = (sender, sender, start_date_formatted, end_date_formatted)
    start_time = time1.time()
    conn = pymysql.connect(host='localhost', user='root', password='18921190757ytk', database='zion')
    params = (sender, sender, start_date_formatted, end_date_formatted)
    with conn.cursor() as cursor:
        sql_query = """
            SELECT * FROM parased_emails
            WHERE (sender = %s OR reciver = %s)
            AND date BETWEEN %s AND %s order by date
            """
        cursor.execute(sql_query, (sender, sender, start_date_formatted, end_date_formatted))
        result = cursor.fetchall()
    parased_emails_df = pd.DataFrame(result)
    parased_emails_df.columns = ['message_id', 'date', 'sender', 'reciver', 'content', 'index']
    email_id_list = parased_emails_df['index'].tolist()

    return email_id_list, parased_emails_df
def select5(sender, start_date_str, end_date_str, query_content):
    email_index_id, email_df = select4(sender, start_date_str, end_date_str)
    query_embedding = model.encode(query_content)
    filtered_id = [int(i) for i in email_index_id]
    search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
    top_k = 1000
    results = collection.search(
        data=[query_embedding],
        anns_field="embedding",
        param=search_params,
        limit=top_k,
        expr=f'email_id in {filtered_id}',
        output_fields=["email_id"]
    )
    result_ids = []
    distances = [hit.distance for result in results for hit in result]
    # 计算平均值和标准差
    mean_distance = np.mean(distances)
    std_distance = np.std(distances)
    # 使用平均值加上一个标准差作为阈值
    threshold_std = mean_distance + std_distance
    # 计算四分位数和 IQR
    Q1 = np.percentile(distances, 25)
    Q3 = np.percentile(distances, 75)
    IQR = Q3 - Q1
    # 使用 IQR 方法计算上限阈值
    threshold_iqr = Q3 + 1.5 * IQR

    # 可以根据需要选择一种方法作为阈值
    threshold = max(threshold_std, threshold_iqr)
    for result in results:
        for hit in result:
            if hit.distance > threshold:
                result_ids.append(hit.entity.get('email_id'))
    result_df = email_df.loc[email_df['index'].isin(result_ids)]
    return result_df
