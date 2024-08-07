import random
import pandas as pd
import requests
app3 = "fastgpt-zuCNJUeY9PNbyehVjROcM1P0g8gYIgK9b5rc4N1gKlE8vrMXdKLDkp73J7zTzRAL7"
def quickGroup(res,target):
    res['different_email'] = res.apply(
        lambda row: row['reciver'] if row['sender'] == target else row['sender'], axis=1)
    def concatenate_group(group):
        return '|'.join(['date:{} sender:{} reciver:{} content:{}'.format(row['date'], row['sender'], row['reciver'],row['content']) for idx, row in group.iterrows()])
    res = res.groupby('different_email').apply(concatenate_group).reset_index()
    return res
def chat_fast(content,chatId,appid):
    url = 'https://api.fastgpt.in/api/v1/chat/completions'
    headers = {
        'Authorization': 'Bearer '+appid,
        'Content-Type': 'application/json'
    }
    data = {
        "chatId": chatId,
        "stream": False,
        "detail": False,
        "messages": [
            {
                "content": content,
                "role": "user"
            }
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    response_json = response.json()
    message=response_json['choices'][0]['message']['content']
    return message

def apply(dataframe,sender):
    #随机生成正整数
    int_number=random.randint(1,1000)

    return chat_fast(dataframe,int_number,app3)

def conclusion_dataframe(content):
    parsed_data = []
    for item in content:
        parts = item.split(' ')
        date_day = parts[0].split(':')[1]
        date_time = parts[1]
        date = f"{date_day} {date_time}"
        sender = parts[2].split(':')[1]
        receiver = parts[3].split(':')[1]
        content = ' '.join(parts[4:]).split(':')[-1]
        parsed_data.append({'date': date, 'sender': sender, 'receiver': receiver, 'content': content})
    df=pd.DataFrame(parsed_data)
    return df