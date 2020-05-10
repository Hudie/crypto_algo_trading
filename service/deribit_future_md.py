# -*- coding: utf-8 -*-

from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json
import pickle
import time
import asyncio
from crypto_trading.config import *



activechannels = set()
hourlyupdated = False

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
    "method" : "public/set_heartbeat",
    "params" : {
        "interval" : 10
        # "interval" : 15
    },
    "jsonrpc" : "2.0",
    "id" : MSG_HEARTBEAT_ID
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

    async def pub_msg(self):
        # get marketdata from exchange socket, then pub to zmq
        try:
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                self.logger.info('Connected to deribit websocket server')
                global activechannels, hourlyupdated
                # set heartbeat to keep alive
                await websocket.send(json.dumps(heartbeat))
                await websocket.recv()
                
                # get instruments and then update channels
                await websocket.send(json.dumps(instruments))
                response = json.loads(await websocket.recv())
                for i in response['result']:
                    self.pubserver.send_string(json.dumps({'type': 'instrument',
                                                           'data': str(pickle.dumps(parse_deribit_instrument(i)))}))
                    for j in ('trades', 'ticker', 'book'):
                        activechannels.add('.'.join([j, i['instrument_name'], 'raw']))
                subscribe['params']['channels'] = list(activechannels)
                await websocket.send(json.dumps(subscribe))
                
                # it is very important here to use 'self.state' to control start/stop!!!
                hourlyupdated = True
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 30:
                        raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped')
                    
                    # update instruments every hour
                    if time.gmtime().tm_min == 5 and hourlyupdated == False:
                        # self.logger.info('Fetching instruments hourly ******')
                        await websocket.send(json.dumps(instruments))
                        self.logger.info('future md send instruments request')
                        hourlyupdated = True
                    elif time.gmtime().tm_min == 31 and hourlyupdated == True:
                        hourlyupdated = False
                    else:
                        pass
                    
                    response = json.loads(await websocket.recv())
                    # self.logger.info(response)
                    # need response heartbeat to keep alive
                    if response.get('method', '') == 'heartbeat':
                        if response['params']['type'] == 'test_request':
                            lastheartbeat = time.time()
                            await websocket.send(json.dumps(test))
                            self.logger.info('future md send heartbeat test back')
                        else:
                            pass
                            # self.logger.info('Serverside heartbeat: ' + str(response))
                    elif response.get('id', '') == MSG_INSTRUMENTS_ID:
                        newchannels = set()
                        for i in response['result']:
                            for j in ('trades', 'ticker', 'book'):
                                newchannels.add('.'.join([j, i['instrument_name'], 'raw']))
                        if len(newchannels.difference(activechannels)) > 0:
                            self.logger.info('There are new channels as following:')
                            self.logger.info(str(newchannels.difference(activechannels)))
                            subscribe['params']['channels'] = list(newchannels)
                            await websocket.send(json.dumps(subscribe))
                            self.logger.info('future md send subscribe')
                            unsubscribe['params']['channels'] = list(activechannels.difference(newchannels))
                            await websocket.send(json.dumps(unsubscribe))
                            self.logger.info('future md send unsubscribe')
                            newinstruments = set()
                            for i in newchannels.difference(activechannels):
                                newinstruments.add(i.split('.')[1])
                            for i in response['result']:
                                if i['instrument_name'] in newinstruments:
                                    self.pubserver.send_string(json.dumps({'type': 'instrument',
                                                                           'data': str(pickle.dumps(parse_deribit_instrument(i)))}))
                            activechannels = newchannels
                    elif response.get('params', ''):
                        # self.logger.info(str(response['params']['data']))
                        if response['params']['channel'].startswith('trades'):
                            for i in response['params']['data']:
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
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
        # except websockets.exceptions.ConnectionClosedError:
        #     await self.pub_msg()
        except Exception as e:
            self.logger.exception(e)
            await asyncio.sleep(1)
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
