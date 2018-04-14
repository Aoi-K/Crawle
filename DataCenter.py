# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
producer = KafkaProducer(bootstrap_servers=['192.168.1.138:9092'])#Create a connection to this Broker's Producer
content = input('Please enter the ip or domain:')
for _ in range(3):
    producer.send('ip',content)#This way of sending does not have a specify Partition,Kafka will evenly write these messages into five Partitons.
    producer.flush()
producer.close()


