import requests
import queue
import threading
import csv
import json
from lxml import etree
# 获取所有电话号码
#电话号段
def get_fund_code(num_list):
    fund_code_list=[]
    html = requests.get(url)
    html = etree.HTML(html.text)
    num_list = html.xpath('//dd/a/text()')
    for i in num_list:
        num_start="0001"
        num_end="9999"
        code_list=[i for i in range(int(i+num_start),int(i+num_end))]
        for c in code_list:
            fund_code_list.append(c)
    return fund_code_list

def get_fund_data():
    # 当队列不为空时
    while (not fund_num_queue.empty()):
        # 从队列读取一个电话代码
        # 读取是阻塞操作
        fund_num = fund_num_queue.get()
        # 获取一个代理，格式为ip:端口
        # 获取一个随机user_agent和Referer
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; U; CPU iPhone OS 5_1_1 like Mac OS X; en) AppleWebKit/534.46.0 (KHTML, like Gecko) CriOS/19.0.1084.60 Mobile/9B206 Safari/7534.48.3",
            "Connection": "close"
        }
        datas = {
            # "CustomerName":"Jack",
            "mobile": fund_num
                }
        proxies={
            "http":"http://101.236.57.99:8866"}
        url = "https://m.tkvip.com/publicx/check_mobile.html?"
        # 使用try、except来捕获异常
        # 如果不捕获异常，程序可能崩溃
        try:
            # 使用代理访问 proxies={"http": proxy},
            req = requests.post(url, headers=headers,data=datas)

            # 没有报异常，说明访问成功
            # 获得返回数据
            responsetext = json.loads(req.text)

            if responsetext["msg"] == "该手机号已注册":
                mutex_lock.acquire()
                print(datas)
                with open('./tel_data.csv', 'a+', encoding='utf-8') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow(datas.values())
                #释放锁
                mutex_lock.release()
            else:
                print(datas.values())
        except Exception as e:
            print(e)


if __name__ == '__main__':

    # num_segment = [i for i in num_list]
    # 获取所有电话号码
    url = 'https://www.guisd.com/ss/sichuan/guangyuan/'
    fund_num_list = get_fund_code(url)

    # 将所有电话号码放入先进先出FIFO队列中
    # 队列的写入和读取都是阻塞的，故在多线程情况下不会乱
    # 在不使用框架的前提下，引入多线程，提高爬取效率
    # 创建一个队列
    fund_num_queue = queue.Queue(len(fund_num_list))
    for i in range(len(fund_num_list)):
        fund_num_queue.put(fund_num_list[i])

    # 创建一个线程锁，防止多线程写入文件时发生错乱
    mutex_lock = threading.Lock()
    # 线程数为50，在一定范围内，线程数越多，速度越快
    for i in range(50):
        t = threading.Thread(target=get_fund_data, name='LoopThread' + str(i))
        t.start()
