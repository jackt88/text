import json
import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib3
import time

urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings()
# 注册时间1-5年，注册资金10000万以上。
"""
location  
510104锦江区 510105青羊区  510106金牛区  510107武侯区  510108成华区  510112龙泉驿区 510113青白江区 510114新都区 510115温江区
510116双流区 510117郫都区  510121金堂县  510129大邑县  510131浦江县  510132新津县  510181都江堰市  510182彭州市  510183邛崃市
"""

# 连接数据库
engine = create_engine(
    'mysql+pymysql://xt:XIONGtao80907@rm-m5e61296sm8tj38fbto.mysql.rds.aliyuncs.com/gsxx?charset=utf8mb4')


def insert_sql(data):
    # 使用try...except..continue避免出现错误，运行崩溃
    try:
        # jjcc_data 数据库表名
        data.to_sql("gsxx_data2", engine, if_exists='append', chunksize=1000)
    except Exception as e:
        print(e)


def get_data(address, page):
    try:
        url = "https://clues.cn/api/opensearch/search"
        heads = {
            "Host": "clues.cn",
            "Connection": "keep-alive",
            "Content-Length": "408",
            "Accept": "application/json,text/plain, */*",
            "Sec-Fetch-Dest": "empty",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZTg2ZDhmYWUwYWM2YTU1M2FlMWJjNDkiLCJyb2xlIjoibWFpbmFjY291bnQiLCJzZXF1ZW5jZSI6MiwiZHRNb2JpbGVTZXF1ZW5jZSI6MCwic3RhdHVzIjoxLCJkaXNhYmxlIjpmYWxzZSwidHlwZSI6OCwiYWdlbnRfdHlwZSI6MCwiYWdlbnRfaWQiOiIiLCJkdHlwZSI6NCwiaWF0IjoxNTg2MTg0NTk0LCJleHAiOjE1ODY1MzQzOTl9.4xm6dpeXWAOmpjo1tLSlh8JDHvTfKrDqtHQ8Rr1Hd1A",
            "X-AK-UID": "5e86d8fae0ac6a553ae1bc49",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://clues.cn",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Referer": "https://clues.cn/search",
            "Accept-Encoding": "gzip,deflate,br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "cookie": "_uab_collina=158589568261941297595166;gotopc=true;Hm_lvt_52ec34d38adb4a7123e14a20870d743a=1585895683,1586184586;Hm_lpvt_52ec34d38adb4a7123e14a20870d743a=1586185171",
            "User-Agent": "Mozilla/5.0 (Macintosh;Intel Mac OS X 10_15_4)AppleWebKit/537.36(KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
        }

        data = {
            "keyword": "",
            "filter": '{"location":["%s"],"industryshort":[],"registercapital":"4","establishment":"2","entstatus":"1","enttype":"1","contact":["1001"],"finance":[],"trademark":"0","patent":"0","shixin":"0","tenders":"0","mobileApp":"0","sem":"0","scale":"0","website":"0","employment":"0"}' % address,
            "scope": "",
            "sortType": "0",
            "pagesize": "50",
            "page": page
        }
        re = json.loads(requests.post(url, headers=heads, data=json.dumps(data), verify=False).text).get("data").get(
            "items")
        data = pd.DataFrame(re)
        data = data[["companyname_ws", "value", "id", "enttype", "esdate", "industry", "registercapital", "lat", "lon",
                     "address", "businessAddress",
                     "legalperson", "entstatus", "products"]]
        insert_sql(data)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    list = ["510104", "510105", "510106", "510107", "510108", "510112", "510113", "510114","510115",
            "510116", "510117", "510121", "510129", "510131", "510132", "510181", "510182", "510183"]
    for x in list:
        time.sleep(120)
        for i in range(101):
            get_data(x,i)
            print("正在写入%s第%s页"% (x,i))


