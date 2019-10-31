# -*- coding: utf-8 -*-

from tornado.platform.asyncio import AsyncIOMainLoop
from enum import Enum
import logging
import zmq.asyncio
import asyncio
import tornado
import json
import logging



logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler('services.log', mode='a')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s : %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)


class ServiceState(Enum):
    init 	= 'init'
    starting 	= 'starting'
    started 	= 'started'
    stopping 	= 'stopping'
    stopped 	= 'stopped'


class ServiceBase(object):
    """ building block of feed, om, strategy services. 
    """
    
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        self._set_state(ServiceState.init)

        # service id, and all services should have different sid
        self.sid = 'servicebase'

        self.ctx = zmq.asyncio.Context()
        # REQ client connected to monitor for heartbeating
        self.reqclient = self.ctx.socket(zmq.REQ)
        self.reqclient.connect('tcp://localhost:8810')
        # SUB client for subscribing messages from monitor
        self.subclient = self.ctx.socket(zmq.SUB)
        self.subclient.connect('tcp://localhost:8820')
        self.subclient.setsockopt_string(zmq.SUBSCRIBE, '')

    def _set_state(self, state):
        if state in ServiceState:
            self.state = state
            self.logger.info('set server state to %s' % state)
        else:
            self.logger.error('invalid server state, need in ServiceState')
            raise RuntimeError('invalid server state, need in ServiceState')
                                   
    async def start(self):
        self._set_state(ServiceState.starting)
        self.logger.info('service starting')
        await self.run()
        
    async def stop(self):
        self._set_state(ServiceState.stopped)
        self.logger.info('service stopped')   
    
    def status(self):
        return self.state.value

    # for PUB services to publish messages to zmq PUB socket
    async def pub_msg(self):
        pass

    # for SUB services to consume messages from subscribed zmq PUBs
    async def sub_msg(self):
        pass

    # listening from monitor PUB socket for remote control
    async def on_control_msg(self):
        while True:
            msg = json.loads(await self.subclient.recv_string())
            if msg['sid'] == self.sid:
                if msg['action'] == 'stop':
                    await self.stop()
                elif msg['action'] == 'start':
                    await self.run()

    # heartbeat, used to periodically update service status
    async def heartbeat(self, infos):
        while True:
            infos.update({'sid': self.sid, 'type': 'heartbeat', 'state': self.state.value})
            await self.reqclient.send_string(json.dumps(infos))
            msg = await self.reqclient.recv_string()
            await asyncio.sleep(10)
    
    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            # do nothing here in base class
            # await self.pub_msg()
            # await self.sub_msg()

def start_service(service, infos):
    AsyncIOMainLoop().install()
    loop = tornado.ioloop.IOLoop.current()
    loop.spawn_callback(service.start)
    loop.spawn_callback(service.on_control_msg)
    loop.spawn_callback(service.heartbeat, infos)
    loop.start()


if __name__ == '__main__':
    service = ServiceBase('servicebase')
    start_service(service, {})
