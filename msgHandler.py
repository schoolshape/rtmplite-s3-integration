#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import pika, logging, socket, os
from RabbitMQAuth import *

def getIpAddr():
    ip = os.popen('hostname -i').read()
    return ip[:-2] # "127.0.0.1 \n".[:-2] == "127.0.0.1"

ip = getIpAddr()

class RabbitMQSender():
    def __init__(self):
        credentials = pika.PlainCredentials(USER, AUTH)
        conn_params = pika.ConnectionParameters(host=MQ_HOST, virtual_host=V_HOST, credentials=credentials)
        connection = pika.BlockingConnection(conn_params)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE, type='topic')
    def message(self, msg):
        self.channel.basic_publish(exchange=EXCHANGE,
            routing_key='stream.{0}.{1}.{2}'.format(ip, msg['module'], msg['level']),
            body=msg['message'])

class rtmpLogHandler(logging.Handler):
    def __init__(self):
        self.broadcaster = RabbitMQSender()
        self.level = 0
        self.filters = []
        self.lock = 0
    def emit(self, body):
        #print '----------', body.filename
        msg = {
            "message": body.getMessage(),
            "level": body.levelname,
            "module":body.filename,
            "lineno":body.lineno,
            "exception":body.exc_info
            }
        self.broadcaster.message(msg)
