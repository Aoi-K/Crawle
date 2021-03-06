# -*- coding: utf-8 -*-
import redis
import Redis
import Mongo
import datetime
import urllib.parse
import urllib.request
from parsel import Selector
from kafka import KafkaConsumer
from pymongo import MongoClient
from multiprocessing import Pool
import threading,random,collections,json

class Database:# Create a Redis database
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

def open_url(url):
    iplist = ['123.207.17.166:80', '111.20.250.232:8081', '183.232.188.103:8080', '221.232.195.6:808','220.249.185.178:9999']
    proxy_support = urllib.request.ProxyHandler({'http': random.choice(iplist)})
    opener = urllib.request.build_opener(proxy_support)
    opener.addheaders = [('User-Agent','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36')]
    response = urllib.request.urlopen(url)
    html = response.read().decode('utf-8')
    s=Selector(text=html)
    return s

def main():
    consumer = KafkaConsumer('Test', group_id='test1', bootstrap_servers=['192.168.1.138:9092'])
    for message in consumer:# Read the massage from kafka(user's query instruction)
        recv = "%s:%d:%d: key=%s value=%s" % (
        message.topic, message.partition, message.offset, message.key, message.value)
        content = message.value
        url = 'http://whois.chinaz.com/' + content
        s = open_url(url)
        div = s.xpath('//*[@id="sh_info"]/li/div[starts-with(@class,"fl WhLeList-left")]/text()').extract()
        span = s.xpath(
            '//div[contains(@class,"fl WhLeList-left") or contains(@class,"block ball") or contains(@class,"fl WhLeList-right") or contains(@class,"fr WhLeList-right")]/span/text()').extract()
        domain = s.xpath('//*[@id="sh_info"]/li[1]/div[2]/p[2]/a/text()').extract()
        if len(div) == 10:
            DNS = s.xpath('//*[@id="sh_info"]/li[10]/div[2]/text()[position()>0]').extract()
            state = s.xpath('//*[@id="sh_info"]/li[11]/div[2]/p[position()>0]/span/text()[1]').extract()
        else:
            DNS = s.xpath('//*[@id="sh_info"]/li[9]/div[2]/text()[position()>0]').extract()
            state = s.xpath('//*[@id="sh_info"]/li[10]/div[2]/p[position()>0]/span/text()[1]').extract()
        data = collections.OrderedDict()
        key = span[0]
        value = content
        data[key] = value
        key = '其他常用域名'
        value = domain
        data[key] = value
        for i in range(1, len(span)):
            key = div[i - 1]
            value = span[i]
            data[key] = value
        key = 'DNS'
        value = DNS
        data[key] = value
        key = '状态'
        value = state
        data[key] = value
        my_set = connet()
        insert(my_set, data)
        add_data(data)

if __name__ == '__main__':
    pool = Pool()  # Create a process pool
    pool.map(main, (i * 10 for i in range(10)))