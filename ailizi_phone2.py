import json
import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib3
import time
urllib3.disable_warnings()

requests.packages.urllib3.disable_warnings()
# 连接数据库
engine = create_engine(
    'mysql+pymysql://xt:XIONGtao80907@rm-m5e61296sm8tj38fbto.mysql.rds.aliyuncs.com/gsxx?charset=utf8mb4')


def select_sql():
    #sql_cmd = "select id from gsxx_data"
    sql_cmd="select pid from gsxx_data2 where pid  not in(select distinct pid from gsxx_phone2);"
    df = pd.read_sql(sql_cmd, engine)
    # print(df["id"].values)
    # print(len(df["id"].values))
    return df["pid"].values


def insert_sql(data):
    # 使用try...except..continue避免出现错误，运行崩溃
    try:
        # jjcc_data 数据库表名
        data.to_sql("gsxx_phone2", engine, if_exists='append')
    except Exception as e:
        print(e)


def get_data(pid):
    # url = "https://clues.cn/api/opensearch/marketReport?id=%s&market_company=%s&market_source=search"%(pid,pid)
    # url = "https://clues.cn/api/gonghai/clickContact?pid=%s&from=market" % pid
    url="https://clues.cn/api/opensearch/marketReport?id=%s&market_company=%s&market_source=search"%(pid,pid)

    heads = {
        "Host": "clues.cn",
        "Connection": "keep-alive",
        "Accept": "application/json,text/plain, */*",
        "Sec-Fetch-Dest": "empty",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZTg2ZDhmYWUwYWM2YTU1M2FlMWJjNDkiLCJyb2xlIjoibWFpbmFjY291bnQiLCJzZXF1ZW5jZSI6MywiZHRNb2JpbGVTZXF1ZW5jZSI6MCwic3RhdHVzIjoxLCJkaXNhYmxlIjpmYWxzZSwidHlwZSI6OCwiYWdlbnRfdHlwZSI6MCwiYWdlbnRfaWQiOiIiLCJkdHlwZSI6MywiaWF0IjoxNTg2NTgxMjMzLCJleHAiOjQ3MTA1ODYyMDB9.2k6fKGsmJ0x5fP0VPvXZu7OmN_MNCCaRk58CW6uObmY",
        "X-AK-UID": "5e86d8fae0ac6a553ae1bc49",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Referer": "https://clues.cn/report/%s?&source=search" % pid,
        "Accept-Encoding": "gzip,deflate,br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "cookie": "_uab_collina=158589568261941297595166;gotopc=true;Hm_lvt_52ec34d38adb4a7123e14a20870d743a=1585895683,1586184586,1586493981;Hm_lpvt_52ec34d38adb4a7123e14a20870d743a=1586581350",
        "User-Agent": "Mozilla/5.0 (Macintosh;Intel Mac OS X 10_15_4)AppleWebKit/537.36(KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "If-None-Match":'W/"7ef-DC8/TeTVSgpnSgW39rM/9p7eIfw"'
    }
    try:
        re = json.loads(requests.get(url, headers=heads, verify=False).text).get("data").get("contacts")
        items = []
        for i in list(re):
            for o in i["data"]:
                items.append([pid, o["sourceName"], i["value"]])
        data = pd.DataFrame(items, columns=["pid", "source", "phone"])
        insert_sql(data)
    except Exception as e:
        print(e, pid)


if __name__ == '__main__':
    for i in select_sql():
        time.sleep(5)
        get_data(i)


