# -*- coding: utf-8 -*-

# from utility.enum import enum
from tornado.platform.asyncio import AsyncIOMainLoop
from service.base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import json



class DeribitMDConsumer(ServiceBase):
    
    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)

        # servie id used for control
        self.sid = 'sid002'
        
        # subscribe data from deribitmd PUB server
        self.msgclient = self.ctx.socket(zmq.SUB)
        self.msgclient.connect('tcp://localhost:9000')
        self.msgclient.setsockopt_string(zmq.SUBSCRIBE, '')

        self.msgclient2 = self.ctx.socket(zmq.SUB)
        self.msgclient2.connect('tcp://localhost:9001')
        self.msgclient2.setsockopt_string(zmq.SUBSCRIBE, '')
        
    # deal with data source 1
    async def sub_msg(self):
        while self.state == ServiceState.started:
            msg = await self.msgclient.recv_string()
            # deal with the coming msg
            print(msg)

    # deal with data source 2
    async def sub_msg2(self):
        while self.state == ServiceState.started:
            msg = await self.msgclient2.recv_string()
            # deal with the coming msg
            print(msg)

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            # run sub_msg & sub_msg2 concurrently
            await asyncio.gather(self.sub_msg(), self.sub_msg2())


    
if __name__ == '__main__':
    service = DeribitMDConsumer('mdconsumer')
    start_service(service, {})
