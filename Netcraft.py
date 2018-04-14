# -*- coding: utf-8 -*-
import Mongo
import redis
import Redis
import datetime
import urllib.request
from parsel import Selector
from kafka import KafkaConsumer
from pymongo import MongoClient
from multiprocessing import Pool
import threading,random,collections

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
    client = MongoClient('192.168.1.138', 27017)   # Establish a connection
    db = client.test  # Establish a connection to the databas test, automatically establish if there is not a database called test 
    my_set = db.test_set  # Use test_set collectio, automatically establish if there doesn't have
    return my_set


def insert(my_set, set):
    my_set.save(set)


def serach(my_set, name):
    for i in my_set.find(name):  # Serach by name
        print(i)
    print(my_set.find_one(name))


def serach_all(my_set):
    for i in my_set.find():  # Show and print all information 
        print(i)
        print(i)


def delete(my_set):
    pass


def update(my_set, name, new):
    my_set.update(name, {'$set': new})

def open_url(url):
    iplist = ['123.207.17.166:80', '111.20.250.232:8081', '183.232.188.103:8080', '221.232.195.6:808',
              '220.249.185.178:9999']
    proxy_support = urllib.request.ProxyHandler({'http': random.choice(iplist)})
    opener = urllib.request.build_opener(proxy_support)
    opener.addheaders = [('User-Agent',
                          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36')]
    response = urllib.request.urlopen(url)
    html = response.read().decode('utf-8')
    s = Selector(text=html)
    return s

def match_columns(th,td):
    h=collections.OrderedDict()
    temp = (range(len(th)))
    j=0
    for i in range(len(td)):
        if td[i] != '\n' and td[i] != '\n\n' and td[i] != '\n\n\n':
            temp[j]=td[i]
            j=j+1
    for i in range(len(th)):
        h[th[i]] = temp[i]
    return h

def match_rows(th,td,title):
    h = collections.OrderedDict()
    if len(th)==0:
        h[title[0]]='none'
    else:
        temp = []
        for i in range(len(td)):
            if '\n' in td[i]:
                i = i + 1
            else:
                temp.append(td[i])
        mod = len(temp) % len(th)
        if mod != 0:
            for i in range(mod):
                temp.append(' ')
        k = 0
        for i in range(len(temp) / len(th)):
            list = collections.OrderedDict()
            for j in range(len(th)):
                list[th[j]] = temp[k]
                k = k + 1
            key = '%s%s%s' % (title[0], ' ', str(i + 1))
            h[key] = list
    return h
def site_tech(th,td,title):
    h = collections.OrderedDict()
    if len(th)==0:
        h[title[0]]='none'
    else:
        temp = []
        for i in range(len(td)):
            if '\n' in td[i]:
                i = i + 1
            else:
                temp.append(td[i])
        k = 0
        for i in range(len(title)):
            list = collections.OrderedDict()
            for j in range(len(th)):
                list[th[j]] = temp[k]
                k = k + 1
            h[title[i]] = list
    return h

def main():
    consumer = KafkaConsumer('Test', group_id='test1', bootstrap_servers=['192.168.1.138:9092'])
    for message in consumer:
        recv = "%s:%d:%d: key=%s value=%s" % (
        message.topic, message.partition, message.offset, message.key, message.value)
        content = message.value
        url = 'https://toolbar.netcraft.com/site_report?url=' + content
        s = open_url(url)
        report = collections.OrderedDict()
        table = s.xpath('//h2/text()').extract()
        index = 0

        bth = s.xpath('//*[@id="background_table"]/div[2]/table/tbody/tr[position()>0]/th/text()').extract()
        btd = s.xpath('//*[@id="background_table"]/div[2]/table/tbody/tr[position()>0]/td//text()').extract()
        report[table[index]] = match_columns(bth, btd)
        index = index + 1

        nth = s.xpath('//*[@id="network_table"]/div[2]/table/tbody/tr[position()>0]/th/text()').extract()
        ntd = s.xpath('//*[@id="network_table"]/div[2]/table/tbody/tr[position()>0]/td//text()').extract()
        report[table[index]] = match_columns(nth, ntd)
        index = index + 1

        hhth = s.xpath('//*[@id="history_table"]/div[2]/table/thead/tr/th/text()').extract()
        hhtd = s.xpath('//*[@id="history_table"]/div[2]/table/tbody/tr[position()>0]/td//text()').extract()
        title = s.xpath('//*[@id="history_table"]/div[1]/h2/text()').extract()
        report[table[index]] = match_rows(hhth, hhtd, title)
        index = index + 1

        spfth = s.xpath('//*[@id="spf_table"]/div[2]/table/thead/tr/th/text()').extract()
        spftd = s.xpath('//*[@id="spf_table"]/div[2]/table/tbody/tr[contains(@class,"TBtr")]/td//text()').extract()
        title = s.xpath('//*[@id="spf_table"]/div[1]/h2/text()').extract()
        report[table[index]] = match_rows(spfth, spftd, title)
        index = index + 1

        wtth = s.xpath('//*[@id="webbugs_table"]/thead/tr/th//text()').extract()
        wttd = s.xpath('//*[@id="webbugs_table"]/tbody/tr/td//text()').extract()
        title = s.xpath('//*[@id="webbugs"]/div[1]/h2/text()').extract()
        report[table[index]] = match_rows(wtth, wttd, title)
        index = index + 1

        title = s.xpath('//*[@id="technology_table"]/div[2]/ul/li/h3/text()').extract()
        stth = s.xpath('//*[@id="technology_table"]/div[2]/ul/li[1]/div[2]/table/thead/tr/th/text()').extract()
        sttd = s.xpath('//*[@id="technology_table"]/div[2]/ul/li/div[2]/table/tbody/tr/td//text()').extract()
        report[table[index]] = site_tech(stth, sttd, title)

        my_set = connet()
        insert(my_set, report)
        add_data(report)
if __name__ == '__main__':
    pool = Pool()  # Create a process pool
    pool.map(main, (i * 10 for i in range(10)))