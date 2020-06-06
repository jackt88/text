import json
import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib3
import time

urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings()
# 注册时间1-5年，注册资金10000万以上。

# 连接数据库
engine = create_engine(
    'mysql+pymysql://xt:XIONGtao80907@rm-m5e61296sm8tj38fbto.mysql.rds.aliyuncs.com/gsxx?charset=utf8mb4')


def insert_sql(data):
    # 使用try...except..continue避免出现错误，运行崩溃
    try:
        # jjcc_data 数据库表名
        data.to_sql("gsxx_data_cd", engine, if_exists='append')
    except Exception as e:
        print(e)


def get_data(address,zb,nf,page):
    try:
        url = "https://clues.cn/api/opensearch/search"
        heads = {
            "Host": "clues.cn",
            "Connection": "keep-alive",
            "Content-Length": "408",
            "Accept": "application/json,text/plain, */*",
            "Sec-Fetch-Dest": "empty",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZWRiNjg3NTNlMWFiODZmOWE4YzI5YzEiLCJyb2xlIjoibWFpbmFjY291bnQiLCJzZXF1ZW5jZSI6MCwiZHRNb2JpbGVTZXF1ZW5jZSI6MCwic3RhdHVzIjoxLCJkaXNhYmxlIjpmYWxzZSwidHlwZSI6OCwiYWdlbnRfdHlwZSI6MCwiYWdlbnRfaWQiOiIiLCJkdHlwZSI6NCwiaWF0IjoxNTkxNDM3NDI5LCJleHAiOjE1OTE3MTgzOTl9.jnCIjuga9MUN19XyFKQNQvKQfgfmoRM1c13JxXQtAFU",
            "X-AK-UID": "5edb68753e1ab86f9a8c29c1",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://clues.cn",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Referer": "https://clues.cn/search",
            "Accept-Encoding": "gzip,deflate,br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "cookie": "Hm_lvt_52ec34d38adb4a7123e14a20870d743a=1591436590; _uab_collina=159143658966255932283269; gotopc=true; Hm_lpvt_52ec34d38adb4a7123e14a20870d743a=1591439940",
            "User-Agent": "Mozilla/5.0 (Macintosh;Intel Mac OS X 10_15_4)AppleWebKit/537.36(KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
        }
        # establishment：1是1年以内，2是1-5年，3是5-10年，4是10-15年，5是15年以上
        data = {
            "keyword": "",
            "filter": '{"location":["%s"],"industryshort":[],"registercapital":"%s","establishment":"%s","entstatus":"1","enttype":"0","contact":["1001"],"finance":[],"trademark":"0","patent":"0","shixin":"0","tenders":"0","mobileApp":"0","sem":"0","scale":"0","website":"0","employment":"0"}' % (address, zb, nf),
            #"filter":"{\"location\":[\"510104\"],\"industryshort\":[],\"registercapital\":\"1\",\"establishment\":\"1\",\"entstatus\":\"1\",\"enttype\":\"0\",\"contact\":[\"1001\"],\"finance\":[],\"trademark\":\"0\",\"patent\":\"0\",\"shixin\":\"0\",\"tenders\":\"0\",\"mobileApp\":\"0\",\"sem\":\"0\",\"scale\":\"0\",\"website\":\"0\",\"employment\":\"0\"}","scope":"","sortType":0,"pagesize":50,"page":1}
            "scope": "",
            "sortType": "0",
            "pagesize": "50",
            "page": page
        }
        # re=requests.post(url, headers=heads, data=json.dumps(data), verify=False).text
        # print(re)

        re = json.loads(requests.post(url, headers=heads, data=json.dumps(data), verify=False).text).get("data").get("items")
        # print(re)
        data = pd.DataFrame(re)
        data = data[["companyname_ws", "value", "id", "enttype", "esdate", "industry", "lat", "lon","registercapital",
                     "businessAddress", "legalperson", "entstatus", "products"]]

        insert_sql(data)

    except Exception as e:
        print(e)


"""
location  
510104锦江区 510105青羊区  510106金牛区  510107武侯区  510108成华区  510112龙泉驿区 510113青白江区 510114新都区 510115温江区
510116双流区 510117郫都区  510121金堂县  510129大邑县  510131浦江县  510132新津县  510181都江堰市  510182彭州市  510183邛崃市
泸州[510502,510503,510504,510521,510522,510524,510525]江阳区，纳溪区，马龙潭区，泸县，合江县，叙永县，古蔺县

区域代码
"""

if __name__ == '__main__':
    list = ["510104", "510105", "510106", "510107", "510108", "510112", "510113", "510114", "510115",
            "510116", "510117", "510121", "510129", "510131", "510132", "510181", "510182", "510183"]
    for zb in range(1, 6):
        time.sleep(10)
        for nf in range(1, 6):
            for address in list:
                time.sleep(10)
                for page in range(1,101):
                    get_data(address,zb,nf,page)
                    print("正在写入注册资金%s，成立年限%s，区域%s第%s页" % (zb, nf, address, page))
