import json
import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib3
urllib3.disable_warnings()

requests.packages.urllib3.disable_warnings()
#连接数据库
engine = create_engine('mysql+pymysql://xt:XIONGtao80907@rm-m5e61296sm8tj38fbto.mysql.rds.aliyuncs.com/gsxx?charset=utf8mb4')

def select_sql():
    sql_cmd="select id from gsxx_data"
    df = pd.read_sql(sql_cmd, engine)
    # print(df["id"].values)
    # print(len(df["id"].values))
    return df["id"].values




def insert_sql(data):
    # 使用try...except..continue避免出现错误，运行崩溃
    try:
        #jjcc_data 数据库表名
        data.to_sql("gsxx_phone",engine,if_exists='append',chunksize=1000)
    except Exception as e:
        print(e)

def get_data(pid):

    url="https://clues.cn/api/gonghai/clickContact?pid=%s&from=market"% pid

    heads = {
    "Host": "clues.cn",
    "Connection": "keep-alive",

    "Accept": "application/json,text/plain, */*",
    "Sec-Fetch-Dest": "empty",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZTg2ZDhmYWUwYWM2YTU1M2FlMWJjNDkiLCJyb2xlIjoibWFpbmFjY291bnQiLCJzZXF1ZW5jZSI6MiwiZHRNb2JpbGVTZXF1ZW5jZSI6MCwic3RhdHVzIjoxLCJkaXNhYmxlIjpmYWxzZSwidHlwZSI6OCwiYWdlbnRfdHlwZSI6MCwiYWdlbnRfaWQiOiIiLCJkdHlwZSI6NCwiaWF0IjoxNTg2MTg0NTk0LCJleHAiOjE1ODY1MzQzOTl9.4xm6dpeXWAOmpjo1tLSlh8JDHvTfKrDqtHQ8Rr1Hd1A",
    "X-AK-UID": "5e86d8fae0ac6a553ae1bc49",


    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Referer": "https://clues.cn/report/%s?&source=search" % pid,
    "Accept-Encoding": "gzip,deflate,br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "cookie": "_uab_collina=158589568261941297595166;gotopc=true;Hm_lvt_52ec34d38adb4a7123e14a20870d743a=1585895683,1586184586;Hm_lpvt_52ec34d38adb4a7123e14a20870d743a=1586322096",
    "User-Agent": "Mozilla/5.0 (Macintosh;Intel Mac OS X 10_15_4)AppleWebKit/537.36(KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
    }
    try:
        re=json.loads(requests.get(url,headers=heads,verify=False).text).get("data").get("item")

        items = []
        for i in list(re):
            for o in i["data"]:

                items.append([pid,o["sourceName"],i["value"]])
        data = pd.DataFrame(items,columns=["pid","source","phone"])
        insert_sql(data)
    except Exception as e:
        print(e,pid)


if __name__ == '__main__':
    for i in select_sql():
        get_data(i)
