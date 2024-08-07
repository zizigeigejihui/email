import random
import streamlit as st
from datetime import datetime
from search_utils import select2, select1, select3, select4, select5
import pandas as pd
import json
import time
import re
from conclusion import quickGroup, apply, conclusion_dataframe, chat_fast

app1 = "fastgpt-lz9ccquRuRhIweLtIQX93mAZZTXeTT2jl2zCeEndfI9E69DLiutf"
app2 = "fastgpt-bQox5Okz3inwbNkDfDcpboTwuzycOy1rwGSKt4h4rBFvwvTUFui0QiGPJ8x5wZoec"
ID =random.randint(1000,2000) #随机生成正整数

if "messages" not in st.session_state:
    st.session_state.messages = []


# Streamlit app
def emai_search():
    st.title("Email Search")
    # Search options
    search_option = st.radio("选择搜索方式", ("全文搜索", "条件查询"))

    # 高亮显示

    def highlight_text(content, query):
        highlighted_content = re.sub(f"({re.escape(query)})", r'<span style="color:red">\1</span>', content,
                                     flags=re.IGNORECASE)
        return highlighted_content

    # Function to display emails in a readable format
    def display_emails(df, query=""):
        for index, row in df.iterrows():
            with st.expander(f"Sender: {row['sender']} | Receiver: {row['reciver']} | Date: {row['date']}"):
                if query:
                    highlighted_content = highlight_text(row['content'], query)
                else:
                    highlighted_content = row['content']
                st.write(f"**Content:**\n\n{highlighted_content}", unsafe_allow_html=True)

    def parased_emails_query_conclusion(result_df, sender):
        answer = []
        answer_1 = quickGroup(result_df, sender)
        for i in range(len(answer_1)):
            content = answer_1[0][i]
            content_split = answer_1[0][i].split("|")
            content_dataframe = conclusion_dataframe(content_split)
            content_conclusion = apply(content, sender)
            answer.append([content_dataframe, content_conclusion])
        return answer

    def display_emails_conclusion(answer):
        for item in answer:
            content_dataframe = item[0]
            content_conclusion = item[1]
            sender = content_dataframe["sender"][0]
            reciver = content_dataframe["receiver"][0]
            with st.expander(f"Sender: {sender} | Receiver: {reciver} "):
                for index, row in content_dataframe.iterrows():
                    date = row["date"]
                    content_detail = row["content"]
                    st.write(f"**Date:**\n\n{date}")
                    st.write(f"**Content:**\n\n{content_detail}")
                st.write(f"**Conclusion:**\n\n{content_conclusion}")

    # Full-text search
    if search_option == "全文搜索":
        full_text_query = st.text_input("搜索内容")
        if st.button("Search"):
            start_time = time.time()
            result_df = select3(full_text_query)
            print("select3")
            # 查看查询花了多少ms
            print((time.time() - start_time) * 1000)

            if not result_df.empty:
                display_emails(result_df, full_text_query)
            else:
                st.write("No results found.")
    # Conditional search
    elif search_option == "条件查询":
        # Default values for inputs
        default_sender = "phillip.allen@enron.com"
        default_start_date = datetime(2000, 1, 11)
        default_end_date = datetime(2000, 2, 20)
        default_query_content = "resign"

        # Inputs
        sender = st.text_input("Sender or Receiver", default_sender)
        start_date = st.date_input("Start Date", default_start_date)
        end_date = st.date_input("End Date", default_end_date)
        query_content = st.text_input("Query Content", default_query_content)

        # Search button
        if st.button("Search"):
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            start_time = time.time()
            # 如果 query_content 不为空
            if query_content != "":
                #查看查询花了多少ms
                result_df = select2(sender, start_date_str, end_date_str, query_content)
                print("select2")
                # 查看查询花了多少ms
                print((time.time() - start_time) * 1000)
            else:

                _, result_df = select1(sender, start_date_str, end_date_str)
                print("select1")
                # 查看查询花了多少ms
                print((time.time() - start_time) * 1000)

            if not result_df.empty:
                display_emails(result_df, query_content)
                if query_content != "":
                    start_time_1=time.time()
                    reult = parased_emails_query_conclusion(result_df, sender)
                    print("总结时间")
                    print(time.time()-start_time_1)
                    st.title("总结完成")
                    display_emails_conclusion(reult)

            else:
                st.write("No results found.")


def is_valid_json(my_json_str):
    try:
        json_object = json.loads(my_json_str)
    except ValueError as e:
        return False
    return True


def put_message_one(ans):
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_reply = ""
        for char in ans:
            full_reply += char
            message_placeholder.markdown(full_reply)
            time.sleep(0.02)  # 使用 time.sleep() 控制输出速度
        st.session_state.messages.append({"role": "assistant", "content": full_reply})


def put_message_result(group_res):
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_reply = ""
        for index, row in group_res.iterrows():
            ans = chat_fast(row[0], ID, app2)
            for char in ans:
                full_reply += char
                message_placeholder.markdown(full_reply)
                time.sleep(0.01)  # 使用 time.sleep() 控制输出速度
            border = "  \n" + "=" * 40 + "  \n"
            full_reply += border
            message_placeholder.markdown(full_reply)
        st.session_state.messages.append({"role": "assistant", "content": full_reply})


def pagechat():
    st.title(f"邮件交互分析助手")
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    if prompt := st.chat_input("What is up?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        ans = chat_fast(prompt, ID, app1)
        if is_valid_json(ans):
            wkt = json.loads(ans)
            if wkt["Type"] == '1':
                ids, result = select4(wkt["sender"], wkt["start_date"], wkt["end_date"])
                if result.empty:
                    ans = "没有查询到你要求的邮件信息,请重新询问"
                    put_message_one(ans)
                else:
                    group_res = quickGroup(result, wkt["sender"])
                    put_message_result(group_res)
            elif wkt["Type"] == '2':
                result = select5(wkt["sender"], wkt["start_date"], wkt["end_date"], wkt["query"])
                if result.empty:
                    ans = "没有查询到你要求的邮件信息,请重新询问"
                    put_message_one(ans)
                else:
                    group_res = quickGroup(result, wkt["sender"])
                    put_message_result(group_res)
        else:
            put_message_one(ans)


page = st.sidebar.radio('Navigate', ['邮件检索系统', '邮件对话助手'])
if page == '邮件检索系统':
    emai_search()
elif page == '邮件对话助手':
    pagechat()
