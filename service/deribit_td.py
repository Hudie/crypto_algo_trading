# -*- coding: utf-8 -*-

from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import websockets
import json
import pickle
import time



orders = {}

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
        # PUB server for publishing transaction data
        self.pubserver = self.ctx.socket(zmq.PUB)
        self.pubserver.bind('tcp://*:9010')
        # REP server for transaction requests
        self.repserver = self.ctx.socket(zmq.REP)
        self.repserver.bind('tcp://*:9020')

    async def pub_msg(self):
        # get tx data from exchange socket, then store it and pub it to zmq
        try:
            hourlyupdated = False
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                self.logger.info('Connected to deribit websocket server')
                
                # set heartbeats to keep alive
                await websocket.send(json.dumps(heartbeat))
                await websocket.recv()
                # get instruments
                await websocket.send(json.dumps(instruments))
                response = json.loads(await websocket.recv())
                hourlyupdated = True

                # it is very important here to use 'self.state' to control start/stop!!!
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 15:
                        raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped.')
                    
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
                            self.logger.info('Serverside heartbeat: ' + str(response))
                    elif response.get('id', '') == MSG_INSTRUMENTS_ID:
                        pass # here to update instruments
                    elif response.get('id', '') in (MSG_TEST_ID, ):
                        pass # here to pass useless response
                    else:
                        # self.logger.info(str(response['params']['data']))
                        # deal tx response
                        update_tx_msg
                        pub_data
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
        except Exception as e:
            self.logger.exception(e)
            await self.pub_msg()

    # deal with user request from zmq
    async def on_request(self):
        try:
            while self.state == ServiceState.started:
                msg = json.loads(await self.repserver.recv_string())
                # think more about the req/rep data structure!!!
                await repserver.send_string(json.dumps({'msg': 'copy'}))
                self.logger.info(str(msg))
                # msg: {'exid': DERIBIT_EXCHANGE_ID, 'sid': xxx, 'accountid': xxx, 'nonce': 12, 'method': 'buy',
                # 'params': {'instrument_name': 'BTC-11OCT19-9750-C', 'amount': 1, 'type': 'market/limit', 'price':xxx, 'stop_price': xxx, 'trigger': xxx},
                # }
                # store_msg_and_set_status # use redis, install in docker
                msg.update({'status': 'accepted'})
                key = '.'.join([msg[i] for i in ('exid', 'sid', 'accountid', 'nonce')])
                orders[key] = msg
                send_msg_2deribit
                msg.update({'status': 'sent'})
        except Exception as e:
            self.logger.exception(e)
            await self.on_request()
            

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            await asyncio.gather(self.pub_msg(), self.on_request())


    
if __name__ == '__main__':
    service = DeribitMD('deribit-td', 'deribit-td')
    start_service(service, {'port': 9010, 'ip': 'localhost'})
