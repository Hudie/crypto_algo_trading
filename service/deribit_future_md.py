# -*- coding: utf-8 -*-

from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json
import pickle
import time
# from memory_profiler import profile



DERIBIT_CLIENT_ID = "PmyJIl5T"
DERIBIT_CLIENT_SECRET = "7WBI4N_YT8YB5nAFq1VjPFedLMxGfrxxCbreMFOYLv0"
SYMBOL = 'BTC'

MSG_AUTH_ID = 9929
auth = {
    "jsonrpc" : "2.0",
    "id" : MSG_AUTH_ID,
    "method" : "public/auth",
    "params" : {
        "grant_type" : "client_credentials",
        "client_id" : DERIBIT_CLIENT_ID,
        "client_secret" : DERIBIT_CLIENT_SECRET
    }
}

MSG_SUBSCRIBE_ID = 3600
subscribe = {
    "jsonrpc" : "2.0",
    "id" : MSG_SUBSCRIBE_ID,
    "method" : "public/subscribe",
    "params" : {
        "channels" : [
            #"ticker.BTC-11OCT19-9750-C.raw",
        ]
    }
}

MSG_PRIVATE_SUBSCRIBE_ID = 4235
private_subscribe = {
    "jsonrpc" : "2.0",
    "id" : MSG_PRIVATE_SUBSCRIBE_ID,
    "method" : "private/subscribe",
    "params" : {
        "channels" : [
        ]
    }
}

MSG_UNSUBSCRIBE_ID = 8691
unsubscribe = {
    "jsonrpc" : "2.0",
    "id" : MSG_UNSUBSCRIBE_ID,
    "method" : "public/unsubscribe",
    "params" : {
        "channels" : []
    }
}

MSG_HEARTBEAT_ID = 110
heartbeat = {
    "method": "public/set_heartbeat",
    "params": {
        "interval": 10
    },
    "jsonrpc": "2.0",
    "id": MSG_HEARTBEAT_ID
}

MSG_TEST_ID = 8212
test = {
    "jsonrpc" : "2.0",
    "id" : MSG_TEST_ID,
    "method" : "public/test",
    "params" : {}
}

MSG_INSTRUMENTS_ID = 7617
instruments = {
    "jsonrpc" : "2.0",
    "id" : MSG_INSTRUMENTS_ID,
    "method" : "public/get_instruments",
    "params" : {
        "currency" : SYMBOL,
        "kind" : "future",
        "expired" : False
    }
}

  
class DeribitMD(ServiceBase):
    
    def __init__(self, sid, logger_name):
        ServiceBase.__init__(self, logger_name)

        self.sid = sid
        # zmq PUB socket for publishing market data
        self.pubserver = self.ctx.socket(zmq.PUB)
        self.pubserver.bind('tcp://*:9050')

    # @profile
    async def pub_msg(self):
        # get marketdata from exchange socket, then pub to zmq
        try:
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                self.logger.info('Connected to deribit websocket server')
                # set heartbeats to keep alive
                await websocket.send(json.dumps(heartbeat))
                await websocket.recv()
                
                await websocket.send(json.dumps(auth))
                await websocket.recv()

                private_subscribe['params']['channels'] = ['user.portfolio.BTC', 'user.changes.future.BTC.raw']
                await websocket.send(json.dumps(private_subscribe))
                
                # it is very important here to use 'self.state' to control start/stop!!!
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 30:
                        raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped')
                    
                    response = json.loads(await websocket.recv())
                    # need response heartbeat to keep alive
                    if response.get('method', '') == 'heartbeat':
                        if response['params']['type'] == 'test_request':
                            lastheartbeat = time.time()
                            await websocket.send(json.dumps(test))
                        else:
                            pass
                            # self.logger.info('Serverside heartbeat: ' + str(response))
                    elif response.get('id', '') in (MSG_TEST_ID, MSG_SUBSCRIBE_ID, MSG_UNSUBSCRIBE_ID, MSG_PRIVATE_SUBSCRIBE_ID):
                        pass
                    else:
                        if response['params']['channel'].startswith('ticker'):
                            self.pubserver.send_string(
                                json.dumps({'type': 'quote',
                                            'data': str(pickle.dumps(parse_deribit_quote(response['params']['data'])))}))
                        elif response['params']['channel'].startswith('user.portfolio'):
                            # self.logger.info(response['params']['data'])
                            self.pubserver.send_string(
                                json.dumps({'type': 'user.portfolio',
                                            'data': str(pickle.dumps(response['params']['data']))}))
                        elif response['params']['channel'].startswith('user.changes'):
                            self.pubserver.send_string(
                                json.dumps({'type': 'user.changes.future',
                                            'data': str(pickle.dumps(response['params']['data']))}))
                        else:
                            pass
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
        except websockets.exceptions.ConnectionClosedError:
            await self.pub_msg()
        except Exception as e:
            self.logger.exception(e)
            await self.pub_msg()

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            await self.pub_msg()


    
if __name__ == '__main__':
    service = DeribitMD('deribit-future-md', 'deribit-future-md')
    start_service(service, {})
