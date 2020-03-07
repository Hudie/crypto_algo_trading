# -*- coding: utf-8 -*-

from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json
import pickle
import time



activechannels = set()
hourlyupdated = False

DERIBIT_EXCHANGE_ID = 101
DERIBIT_CLIENT_ID = "8VP1NV2u"
DERIBIT_CLIENT_SECRET = "LEnJgX-u5LpROjpOKGVcY_NRbC_nByBIOg-mCflwzMg"
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
                self.logger.info('Connected to deribit websocket server')
                global activechannels, hourlyupdated
                # set heartbeats to keep alive
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
                hourlyupdated = True

                # it is very important here to use 'self.state' to control start/stop!!!
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 30:
                        raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped')
                    
                    # update instruments every hour
                    if time.gmtime().tm_min == 5 and hourlyupdated == False:
                        self.logger.info('Fetching instruments hourly ******')
                        await websocket.send(json.dumps(instruments))
                        hourlyupdated = True
                    elif time.gmtime().tm_min == 31 and hourlyupdated == True:
                        hourlyupdated = False
                    else:
                        pass
                    
                    response = json.loads(await websocket.recv())
                    # need response heartbeat to keep alive
                    if response.get('method', '') == 'heartbeat':
                        if response['params']['type'] == 'test_request':
                            lastheartbeat = time.time()
                            await websocket.send(json.dumps(test))
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
                            unsubscribe['params']['channels'] = list(activechannels.difference(newchannels))
                            await websocket.send(json.dumps(unsubscribe))
                            newinstruments = set()
                            for i in newchannels.difference(activechannels):
                                newinstruments.add(i.split('.')[1])
                            for i in response['result']:
                                if i['instrument_name'] in newinstruments:
                                    self.pubserver.send_string(json.dumps({'type': 'instrument',
                                                                           'data': str(pickle.dumps(parse_deribit_instrument(i)))}))
                            activechannels = newchannels
                    elif response.get('id', '') in (MSG_TEST_ID, MSG_SUBSCRIBE_ID, MSG_UNSUBSCRIBE_ID):
                        pass
                    else:
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
                    if self.state == ServiceState.started:
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
    service = DeribitMD('deribit-md', 'deribit-md')
    start_service(service, {'port': 9000, 'ip': 'localhost'})
