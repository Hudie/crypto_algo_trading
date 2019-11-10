# -*- coding: utf-8 -*-

from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from crypto_foundation.common.constant import Ecn, Broker, MarketDataApi, TradeDataApi
from crypto_foundation.common.account import CryptoTradingAccount
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import websockets
import json
import pickle
import time
import queue



orders = {}
tokens = {}
accounts = []
# key: accountid, value: request queue
requests = {}

DERIBIT_EXCHANGE_ID = Ecn.deribit
SYMBOL = 'BTC'

MSG_AUTH_ID = 9929
auth = {
    "jsonrpc" : "2.0",
    "id" : MSG_AUTH_ID,
    "method" : "public/auth",
    "params" : {
        "grant_type" : "client_credentials",
        "client_id" : "",
        "client_secret" : ""
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

MSG_GET_POSITIONS_ID 	= 2236
MSG_GET_POSITION_ID 	= 404
MSG_BUY_ID		= 5275
MSG_SELL_ID		= 2148
MSG_CANCEL_ID		= 4214
MSG_CANCEL_ALL_ID	= 8748
MSG_GET_ORDER_STATE_ID	= 4316
MSG_GET_OPEN_ORDERS_BY_CURRENCY_ID	= 1953

  
class DeribitTD(ServiceBase):
    
    def __init__(self, sid, logger_name):
        ServiceBase.__init__(self, logger_name)

        self.sid = sid
        # PUB server for publishing transaction data
        self.pubserver = self.ctx.socket(zmq.PUB)
        self.pubserver.bind('tcp://*:9010')
        # REP server for transaction requests
        self.repserver = self.ctx.socket(zmq.REP)
        self.repserver.bind('tcp://*:9020')

    async def pub_msg(self, account):
        # get tx data from exchange socket, then store it and pub it to zmq
        try:
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                self.logger.info('Account %s connected to deribit websocket server' % account.id)
                
                # set heartbeats to keep alive
                await websocket.send(json.dumps(heartbeat))
                res = await websocket.recv()

                # auth
                auth['params']['client_id'] = account.api_public_key
                auth['params']['client_secret'] = account.api_private_key
                await websocket.send(json.dumps(auth))
                res = json.loads(await websocket.recv())
                tokens[account.id] = res['result']['access_token']
                
                # it is very important here to use 'self.state' to control start/stop!!!
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 15:
                        raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped.')

                    # check request queue to send request
                    if account.id in requests:
                        mq = requests[account.id]
                        if mq.qsize() > 0:
                            msg = mq.get()
                            msg.update({'jsonrpc': '2.0', 'method' : 'private/' + msg['method'],
                                        'id': eval('_'.join(('MSG', msg['method'].upper(), 'ID')))})
                            self.logger.info(msg)
                            await websocket.send(json.dumps(msg))
                            lastheartbeat = time.time()

                    # then deal with every received msg
                    task = asyncio.create_task(websocket.recv())
                    done, pending = await asyncio.wait({task}, timeout=0.0001)
                    for t in pending:
                        t.cancel()
                    response = json.loads(done.pop().result()) if done else {}

                    if response:
                        if response.get('method', '') == 'heartbeat':
                            if response['params']['type'] == 'test_request':
                                await websocket.send(json.dumps(test))
                                lastheartbeat = time.time()
                            else:
                                self.logger.info('Serverside heartbeat: ' + str(response))
                        elif response.get('id', '') in (MSG_TEST_ID, ):
                            pass
                        else:
                            # deal tx response
                            self.logger.info(response)
                            await self.pubserver.send_string(json.dumps({'accountid': account.id, 'result': response}))
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg(account)
        except Exception as e:
            self.logger.exception(e)
            await self.pub_msg(account)

    # deal with user request from zmq
    async def on_request(self):
        try:
            while self.state == ServiceState.started:
                msg = json.loads(await self.repserver.recv_string())
                await self.repserver.send_string(json.dumps({'msg': 'copy'}))
                self.logger.info('**** Request received:')
                self.logger.info(msg)
                # msg : {'sid': xxx, 'userid': 12, 'accountid': xxx,
                #        'method': 'get_postions',
                #        'params': {'currency': 'BTC', 'kind': 'option'} }

                msg.update({'status': 'accepted'})
                # orders[msg['accountid']].append(msg)
                if msg['accountid'] not in requests:
                    requests[msg['accountid']] = queue.Queue()
                requests[msg['accountid']].put({'method': msg['method'], 'params': msg['params']})
        except Exception as e:
            self.logger.exception(e)
            await self.on_request()
            

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            # init account info
            testaccount = CryptoTradingAccount("deribit_test_account",
                                               Broker.deribit_dma, "testid", "",
                                               MarketDataApi.deribit_md_websocket,
                                               TradeDataApi.deribit_td_websocket,
                                               "8VP1NV2u", "LEnJgX-u5LpROjpOKGVcY_NRbC_nByBIOg-mCflwzMg")
            accounts.append(testaccount)
            
            # create websocket for every account
            for account in accounts:
                asyncio.create_task(self.pub_msg(account))
                # fetch account info, including orders and pos
                requests[account.id] = queue.Queue()
                requests[account.id].put({'method': 'get_positions', 'params': {'currency': 'BTC', 'kind': 'option'}})
                requests[account.id].put({'method': 'get_open_orders_by_currency', 'params': {'currency' : 'BTC'}})
                
            await self.on_request()


    
if __name__ == '__main__':
    service = DeribitTD('deribit-td', 'deribit-td')
    start_service(service, {'port': 9010, 'ip': 'localhost'})
