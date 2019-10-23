# -*- coding: utf-8 -*-

from tornado.platform.asyncio import AsyncIOMainLoop
from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json
import pickle
import time



symbol = 'BTC'
activechannels = set()
hourlyupdated = False

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

subscribe = {
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

unsubscribe = {
    "jsonrpc" : "2.0",
    "id" : 8691,
    "method" : "public/unsubscribe",
    "params" : {
        "channels" : []
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
                global activechannels, hourlyupdated
                # set heartbeats to keep alive
                await websocket.send(json.dumps(heartbeat))
                await websocket.recv()
                # update instruments and then update channels
                await websocket.send(json.dumps(instruments))
                response = json.loads(await websocket.recv())
                for i in response['result']:
                    for j in ('trades', 'ticker', 'book'):
                        activechannels.add('.'.join([j, i['instrument_name'], 'raw']))
                subscribe['params']['channels'] = list(activechannels)
                await websocket.send(json.dumps(subscribe))
                hourlyupdated = True

                # it is very important here to use 'self.state' to control start/stop!!!
                while websocket.open and self.state == ServiceState.started:
                    # update instruments
                    if time.gmtime().tm_min % 2 == 1 and hourlyupdated == False:
                        await websocket.send(json.dumps(instruments))
                        '''
                        response = json.loads(await websocket.recv())
                        print(response)
                        newchannels = set()
                        for i in response['result']:
                            newchannels.add('.'.join([j, i['instrument_name'], 'raw']))
                        if len(newchannels.difference(activechannels)) > 0:
                            subscribe['params']['channels'] = list(newchannels)
                            await websocket.send(json.dumps(subscribe))
                            await websocket.recv()
                            unsubscribe['params']['channels'] = list(activechannels.difference(newchannels))
                            await websocket.send(json.dumps(unsubscribe))
                            # await websocket.recv()
                        '''
                        hourlyupdated = True
                        print('+++++ set hourlyupdated true')
                    elif time.gmtime().tm_min % 2 == 0 and hourlyupdated == True:
                        hourlyupdated = False
                        print('+++++ set hourlyupdated false')
                    else:
                        pass
                    
                    response = json.loads(await websocket.recv())
                    # need response heartbeat to keep alive
                    if response.get('method', '') == 'heartbeat':
                        if response['params']['type'] == 'test_request':
                            await websocket.send(json.dumps(test))
                    elif response.get('id', '') == 7617:
                        print('==========================')
                        print(response['result'])
                        newchannels = set()
                        for i in response['result']:
                            newchannels.add('.'.join([j, i['instrument_name'], 'raw']))
                        if len(newchannels.difference(activechannels)) > 0:
                            subscribe['params']['channels'] = list(newchannels)
                            await websocket.send(json.dumps(subscribe))
                            #unsubscribe['params']['channels'] = list(activechannels.difference(newchannels))
                            #await websocket.send(json.dumps(unsubscribe))
                            activechannels = newchannels
                    elif response.get('id', '') in (8212, 8691, 3600):
                        pass
                    else:
                        # print(response['params']['data'])
                        if response['params']['channel'].startswith('trades'):
                            for i in response['params']['data']:
                                # print(parse_deribit_trade(i))
                                self.pubserver.send_string(json.dumps({'type': 'trade',
                                                                       'data': str(pickle.dumps(parse_deribit_trade(i)))}))
                        elif response['params']['channel'].startswith('ticker'):
                            self.pubserver.send_string(
                                json.dumps({'type': 'quote',
                                            'data': str(pickle.dumps(parse_deribit_quote(response['params']['data'])))}))
                        elif response['params']['channel'].startswith('book'):
                            self.pubserver.send_string(
                                json.dumps({'type': 'book',
                                            'data': str(pickle.dumps(parse_deribit_order_book(response['params']['data'])))}))
                        else:
                            pass
                else:
                    pass
        # except websockets.exceptions.ConnectionClosedError as e:
        except Exception as e:
            print(e)
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