# -*- coding: utf-8 -*-
import redis
import Redis
import Mongo
import urllib.request
from parsel import Selector
from pymongo import MongoClient
from kafka import KafkaConsumer
from multiprocessing import Pool
import datetime,threading,random,os,collections

url = 'http://feed.watcherlab.com/'
ipurl = 'http://www.whatismyio.com'

def agent(ipurl):
    iplist = ['223.215.196.62:8888', '42.48.110.26:80', '113.108.253.195:9797', '183.232.188.105:8080',
              '211.142.22.89:80']
    proxy_support = urllib.request.ProxyHandler({'http': random.choice(iplist)})
    opener = urllib.request.build_opener(proxy_support)
    opener.addheaders = [('User-Agent',
                          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36')]
    opener.open(ipurl)
    response = urllib.request.urlopen(url)
    html = response.read().decode('utf-8')

def open_url(url):
    req = urllib.request.Request(url)
    req.add_header('User-Agent',
                   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36')
    response = urllib.request.urlopen(url)
    html = response.read().decode('utf-8')
    return html

def get_xpath(html):
    return Selector(text=html)

def get_address(s):
    i = 1
    feed_list = []
    while i <= 409:
        print("http://feed.watcherlab.com/" + s.xpath('//a/text()').extract()[i])
        print(s.xpath('//pre/text()').extract()[i])
        feed_list.append(s.xpath('//a/text()').extract()[i])
        i = i + 1
    return feed_list

def download(file_name, dest_dir):
    download_url = 'http://feed.watcherlab.com/' + file_name
    try:
        urllib.urlretrieve(download_url, dest_dir)
    except:
        print("\tError retrieving the URL:", dest_dir)

def connet():
    client = MongoClient('192.168.1.138', 27017)  # Establish a connection
    db = client.test  # Establish a connection to the databas test, automatically establish if there is not a database called test
    my_set = db.test_set  # Use test_set collectio, automatically establish if there doesn't have
    return my_set

def insert(my_set,set):
    my_set.save(set)

def serach(my_set,name):
    for i in my_set.find(name):  # Serach by name
        print(i)
    print(my_set.find_one(name))

def serach_all(my_set):
    for i in my_set.find():  # Show and print all information 
        print(i)

def delete(my_set):
    pass

def update(my_set,name,new):
    my_set.update(name, {'$set':new})

class Database:
    def __init__(self):
        self.host = '192.168.1.138'
        self.port = 6379
        self.write_pool = {}

    def add_write(self, website, city, year, month, day, deal_number):
        key = '_'.join([website, city, str(year), str(month), str(day)])
        val = deal_number
        self.write_pool[key] = val

    def batch_write(self):
        try:
            r = redis.StrictRedis(host=self.host, port=self.port)
            r.mset(self.write_pool)
        except Exception, exception:
            print exception


def add_data(text):
    beg = datetime.datetime.now()
    db = Database()
    for i in range(1, 10000):
        db.add_write(text)
    db.batch_write()
    end = datetime.datetime.now()
    print end - beg

def main():
    agent(ipurl)
    html = open_url(url)
    s = get_xpath(html)
    feed_list = get_address(s)
    readme = {'ipv4': ["关联标签", "恶意IP", "发现时间", "更新时间", "恶意行为", "协议", "情报来源", "可信度", "存活性", "初次发现时间", "国家代码"],
              'cn': ["关联标签", "恶意IP", "发现时间", "更新时间", "恶意行为", "协议", "情报来源", "可信度", "累积信誉", "初次发现时间", "国家代码", "存活性",
                     "操作系统标识", "端口开放"],
              'url': ["关联标签", "恶意URL", "发现时间", "更新时间", "恶意行为", "情报来源", "存活性", "可信度"],
              'dns': ["关联标签", "恶意DNS", "发现时间", "更新时间", "恶意行为", "协议", "情报来源", "可信度"],
              'email': ["关联标签", "恶意E-mail", "发现时间", "更新时间", "恶意行为", "情报来源", "可信度"],
              'proxy': ["关联标签", "可疑Proxy IP", "发现时间", "Proxy标识", "情报来源", "地理位置", "国家代码", "存活性", "操作系统标识", "端口开放"],
              'tor': ["关联标签", "Tor匿名数据IP", "发现时间", "Tor标识", "情报来源", "地理位置", "国家代码"],
              'botnet': ["关联标签", "BotNet IP", "发现时间", "更新时间", "恶意行为", "协议", "情报来源", "可信度", "存活性", "初次发现时间", "累积信誉",
                         "操作系统标识", "端口开放", "国家代码"],
              'fastflux': ["关联标签", "FastFlux IP", "发现时间", "FastFlux 标识", "情报来源", "地理位置", "国家代码", "存活性", "操作系统标识",
                           "端口开放"],
              'c2': ["关联标签", "C2 IP", "情报来源", "发现时间", "C2 标识", "情报来源", "地理位置", "国家代码", "存活性", "操作系统标识", "端口开放"]}
    path = r'/Users/Aoi/Desktop/Watcherlab'
    for f in range(1, len(feed_list)):
        file_name = feed_list[f]
        category=readme[file_name.split('-')[1]]
        dest_dir = os.path.join(path, file_name)
        download(file_name, dest_dir)
        file= open(path+"/watcherlab-2016-12-01/"+file_name, "r")
        lines = file.readlines()  # Read all lines
        for line in lines:
            word=line.split(',')
            data = collections.OrderedDict()
            for i in range(0,len(word)):
                key = category[i]
                value = word[i]
                data[key] = value
            my_set =connet()
            insert(my_set, data)
            add_data(data)
if __name__ == '__main__':
    pool = Pool()  # Create a process pool
    pool.map(main, (i * 10 for i in range(10)))