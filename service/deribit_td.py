# -*- coding: utf-8 -*-

from crypto_foundation.common.constant import Broker, MarketDataApi, TradeDataApi
from crypto_foundation.common.account import CryptoTradingAccount
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import websockets
import json
import time
import queue
from crypto_trading.config import *



tokens = {}
accounts = []
# key: accountid, value: request queue
requests = {}

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
    "method" : "public/set_heartbeat",
    "params" : {
        "interval" : 10
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

MSG_GET_POSITIONS_ID 	= 2236
MSG_GET_POSITION_ID 	= 404
MSG_BUY_ID		= 5275
MSG_SELL_ID		= 2148
MSG_EDIT_ID		= 3725
MSG_CANCEL_ID		= 4214
MSG_CANCEL_ALL_ID	= 8748
MSG_GET_ORDER_STATE_ID	= 4316
MSG_GET_OPEN_ORDERS_BY_CURRENCY_ID	= 1953

MSG_MAP = {MSG_GET_POSITIONS_ID: 'positions',
           MSG_GET_POSITION_ID: 'position',
           MSG_BUY_ID: 'buy',
           MSG_SELL_ID: 'sell',
           MSG_EDIT_ID: 'edit',
           MSG_CANCEL_ID: 'cancel',
           MSG_CANCEL_ALL_ID: 'cancel_all',
           MSG_GET_ORDER_STATE_ID: 'order_state',
           MSG_GET_OPEN_ORDERS_BY_CURRENCY_ID: 'open_orders',
}

def get_random_id():
    count = 0
    while True:
        yield ':'.join([str(int(time.time())), str(count)])
        count += 1
randomid = get_random_id()


  
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

                # private subscribe
                private_subscribe['params']['channels'] = ['user.portfolio.{}'.format(SYMBOL),
                                                           'user.changes.future.{}.raw'.format(SYMBOL)]
                await websocket.send(json.dumps(private_subscribe))
                
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
                            # self.logger.info(msg)
                            await websocket.send(json.dumps(msg))
                            lastheartbeat = time.time()

                    # then deal with every received msg
                    task = asyncio.ensure_future(websocket.recv())
                    done, pending = await asyncio.wait({task}, timeout=1)
                    for t in pending:
                        t.cancel()
                    response = json.loads(done.pop().result()) if done else {}

                    if response:
                        if 'error' in response:
                            self.logger.error(response)
                        if response.get('method', '') == 'heartbeat':
                            if response['params']['type'] == 'test_request':
                                await websocket.send(json.dumps(test))
                                lastheartbeat = time.time()
                        elif response.get('id', '') in MSG_MAP.keys():
                            await self.pubserver.send_string(json.dumps({
                                'accountid': account.id,
                                'type': MSG_MAP[response.get('id')],
                                'data': response.get('result', {}),
                                'error': response.get('error', {})}))
                        elif response.get('params', ''):
                            if response['params']['channel'].startswith('user.portfolio'):
                                self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'user.portfolio',
                                    'data': response['params']['data']}))
                            elif response['params']['channel'].startswith('user.changes.future'):
                                self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'user.changes.future',
                                    'data': response['params']['data']}))
                            else:
                                pass
                        else:
                            pass
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg(account)
        except websockets.exceptions.ConnectionClosedError:
            await self.pub_msg(account)
        except Exception as e:
            self.logger.exception(e)
            await self.pub_msg(account)


    # deal with user request from zmq
    # msg : {'sid': xxx, 'userid': 12, 'accountid': xxx,
    #        'method': 'get_postions',
    #        'params': {'currency': 'BTC', 'kind': 'option'} }
    async def on_request(self):
        try:
            while self.state == ServiceState.started:
                msg = json.loads(await self.repserver.recv_string())
                internalid = ':'.join([msg.get('sid', ''), msg.get('userid', ''), msg['accountid'], next(randomid)])
                await self.repserver.send_string(json.dumps({'internalid': internalid}))
                msg['params'].update({'label': internalid})

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
            quarterly = CryptoTradingAccount(DERIBIT_ACCOUNT_ID,
                                             Broker.deribit_dma, DERIBIT_ACCOUNT_ID, '',
                                             MarketDataApi.deribit_md_websocket,
                                             TradeDataApi.deribit_td_websocket,
                                             DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET)
            accounts.append(quarterly)
            n_quarterly = CryptoTradingAccount(N_DERIBIT_ACCOUNT_ID,
                                               Broker.deribit_dma, N_DERIBIT_ACCOUNT_ID, '',
                                               MarketDataApi.deribit_md_websocket,
                                               TradeDataApi.deribit_td_websocket,
                                               N_DERIBIT_CLIENT_ID, N_DERIBIT_CLIENT_SECRET)
            accounts.append(n_quarterly)
            # create websocket for every account
            for account in accounts:
                asyncio.ensure_future(self.pub_msg(account))
                # fetch account positions
                # requests[account.id] = queue.Queue()
                # requests[account.id].put({'method': 'get_positions', 'params': {'currency': 'BTC', 'kind': 'future'}})
                
            asyncio.ensure_future(self.on_request())


    
if __name__ == '__main__':
    service = DeribitTD('deribit-td', 'deribit-td')
    start_service(service, {})
