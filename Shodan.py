# -*- coding: utf-8 -*-
import redis
import Redis
import Mongo
import urllib2
import datetime
import requests
import threading,random
from kafka import KafkaConsumer
from pymongo import MongoClient
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
def connet():
    client = MongoClient('192.168.1.138', 27017)  # Establish a connection
    db = client.test  # Establish a connection to the databas test, automatically establish if there is not a database called test 
    my_set = db.test_set  # Use test_set collectio, automatically establish if there doesn't have
    return my_set
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


def delete(my_set):
    pass


def update(my_set, name, new):
    my_set.update(name, {'$set': new})

def main():
    consumer = KafkaConsumer('Test', group_id='test1', bootstrap_servers=['192.168.1.138:9092'])
    for message in consumer:
        recv = "%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value)
        ip=message.value
        key = "GThKP1N05OCPK72ULtPjYLOwBhZUZSM0"
        url = 'https://api.shodan.io/shodan/host/' + ip
        result = requests.get(url, params={"key": key}, timeout=(15, 15))
        my_set = connet()
        insert(my_set, result.text)
        add_data(result.text)

