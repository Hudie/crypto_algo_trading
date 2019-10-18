# -*- coding: utf-8 -*-

from tornado.platform.asyncio import AsyncIOMainLoop
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json



symbol = 'BTC'

auth = {
    "jsonrpc" : "2.0",
    "id" : 9929,
    "method" : "public/auth",
    "params" : {
        "grant_type" : "client_credentials",
        "client_id" : "8VP1NV2u",
        "client_secret" : "LEnJgX-u5LpROjpOKGVcY_NRbC_nByBIOg-mCflwzMg"
    }
}

channels = {
    "jsonrpc" : "2.0",
    "id" : 3600,
    "method" : "public/subscribe",
    "params" : {
        "channels" : [
            #"book.BTC-27SEP19-8000-C.10.10.100ms",
            #"ticker.BTC-11OCT19-9500-C.raw",
            #"ticker.BTC-11OCT19-9750-C.raw",
        ]
    }
}

heartbeat = {
    "method": "public/set_heartbeat",
    "params": {
        "interval": 10
    },
    "jsonrpc": "2.0",
    "id": 110
}

test = {
    "jsonrpc" : "2.0",
    "id" : 8212,
    "method" : "public/test",
    "params" : {}
}

instruments = {
    "jsonrpc" : "2.0",
    "id" : 7617,
    "method" : "public/get_instruments",
    "params" : {
        "currency" : symbol,
        "kind" : "option",
        "expired" : False
    }
}

  
class DeribitMD(ServiceBase):
    
    def __init__(self, sid, logger_name):
        ServiceBase.__init__(self, logger_name)

        self.sid = sid
        # zmq PUB socket for publishing market data
        self.pubserver = self.ctx.socket(zmq.PUB)
        self.pubserver.bind('tcp://*:9000')

    async def pub_msg(self):
        # get marketdata from exchange socket, then pub to zmq
        try:
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                # set heartbeats to keep alive
                await websocket.send(json.dumps(heartbeat))
                await websocket.recv()
                # update instruments and then update channels
                await websocket.send(json.dumps(instruments))
                response = json.loads(await websocket.recv())
                for i in response['result']:
                    for j in ('trades', 'ticker', 'book'):
                        channels['params']['channels'].append('.'.join([j, i['instrument_name'], 'raw']))
                await websocket.send(json.dumps(channels))

                # it is very very important here to use 'self.state' to control start/stop
                while websocket.open and self.state == ServiceState.started:
                    response = json.loads(await websocket.recv())
                    # need response heartbeat to keep alive
                    if response.get('method', '') == 'heartbeat':
                        if response['params']['type'] == 'test_request':
                            await websocket.send(json.dumps(test))
                    elif response.get('id', '') in (8212, 3600):
                        pass
                    else:
                        print(response)
                        # send data to zmq
                        self.pubserver.send_string(json.dumps(response))
                else:
                    pass
        except websockets.exceptions.ConnectionClosedError:
            await self.pub_msg()

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            await self.pub_msg()


    
if __name__ == '__main__':
    service = DeribitMD('deribit-md-01', 'deribitmd')
    start_service(service, {'port': 9000, 'ip': 'localhost'})
