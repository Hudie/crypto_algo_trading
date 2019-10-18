# -*- coding: utf-8 -*-

from tornado.platform.asyncio import AsyncIOMainLoop
from service.base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json



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

msg = {
  "jsonrpc" : "2.0",
  "id" : 3600,
  "method" : "public/subscribe",
  "params" : {
    "channels" : [
       #"book.BTC-27SEP19-8000-C.10.10.100ms",
       "ticker.BTC-11OCT19-9500-C.raw",
       "ticker.BTC-11OCT19-9750-C.raw",
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


  
class DeribitMD(ServiceBase):
    
    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)

        self.sid = 'sid001'
        # zmq PUB socket for publishing market data
        self.pubserver = self.ctx.socket(zmq.PUB)
        self.pubserver.bind('tcp://*:9000')

    async def pub_msg(self):
        # get marketdata from exchange socket, then pub to zmq
        try:
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                await websocket.send(json.dumps(heartbeat))
                response = await websocket.recv()
                # print(json.loads(response))
                await websocket.send(json.dumps(auth))
                response = await websocket.recv()
                # print(json.loads(response))
                await websocket.send(json.dumps(msg))
                while websocket.open and self.state == ServiceState.started:
                    response = json.loads(await websocket.recv())
                    # need response heartbeat
                    if response.get('method', '') == 'heartbeat':
                        if response['params']['type'] == 'test_request':
                            await websocket.send(json.dumps({"jsonrpc" : "2.0",
                                                             "id" : 8212,
                                                             "method" : "public/test",
                                                             "params" : {}
                            }))
                    else:
                        print(response)
                        # send data to subscribers
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
    service = DeribitMD('deribitmd')
    start_service(service, {'port': 9000, 'ip': 'localhost'})
