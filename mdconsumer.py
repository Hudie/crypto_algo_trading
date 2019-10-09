# -*- coding: utf-8 -*-

# from utility.enum import enum
from tornado.platform.asyncio import AsyncIOMainLoop
from service.base import ServiceState, ServiceBase
import logging
import zmq.asyncio
import asyncio
import tornado
import json



class DeribitMDConsumer(ServiceBase):
    
    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)

        # servie id used for control
        self.sid = 'sid002'
        
        # SUB data
        self.msgclient = self.ctx.socket(zmq.SUB)
        self.msgclient.connect('tcp://localhost:9000')
        self.msgclient.setsockopt_string(zmq.SUBSCRIBE, '')

    async def sub_msg(self):
        while self.state == ServiceState.started:
            msg = await self.msgclient.recv_string()
            print(msg)

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            print('Here in run body')
            self.state = ServiceState.started
            await self.sub_msg()


    
if __name__ == '__main__':
    service = DeribitMDConsumer('mdconsumer')

    AsyncIOMainLoop().install()
    loop = tornado.ioloop.IOLoop.current()
    loop.spawn_callback(service.start)
    loop.spawn_callback(service.on_control_msg)
    loop.spawn_callback(service.heartbeat, {})
    loop.start()
