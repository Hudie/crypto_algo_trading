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



orders = {}
# tokens = {}
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
MSG_EDIT_ID		= 3725
MSG_CANCEL_ID		= 4214
MSG_CANCEL_ALL_ID	= 8748
MSG_GET_ORDER_STATE_ID	= 4316
MSG_GET_OPEN_ORDERS_BY_CURRENCY_ID	= 1953


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
                await websocket.recv()

                # auth
                auth['params']['client_id'] = account.api_public_key
                auth['params']['client_secret'] = account.api_private_key
                await websocket.send(json.dumps(auth))
                await websocket.recv()
                # res = json.loads(await websocket.recv())
                # tokens[account.id] = res['result']['access_token']
                
                # it is very important here to use 'self.state' to control start/stop!!!
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 30:
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
                    # task = asyncio.create_task(websocket.recv())
                    task = asyncio.ensure_future(websocket.recv())
                    done, pending = await asyncio.wait({task}, timeout=1)
                    for t in pending:
                        t.cancel()
                    response = json.loads(done.pop().result()) if done else {}

                    if response:
                        if response.get('method', '') == 'heartbeat':
                            if response['params']['type'] == 'test_request':
                                await websocket.send(json.dumps(test))
                                lastheartbeat = time.time()
                            # else:
                            #    self.logger.info('Serverside heartbeat: ' + str(response))
                        elif response.get('id', '') in (MSG_TEST_ID, ):
                            pass
                        else:
                            # deal tx response
                            # self.logger.info(response)
                            if response.get('id', '') == MSG_GET_OPEN_ORDERS_BY_CURRENCY_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'open_orders',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_GET_POSITION_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'position',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_GET_POSITIONS_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'positions',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_GET_ORDER_STATE_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'order_state',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_CANCEL_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'cancel',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_CANCEL_ALL_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'cancel_all',
                                    'data': response.get('result', {})}))
                            elif response.get('id', '') == MSG_BUY_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'buy',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_SELL_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'sell',
                                    'data': response['result']}))
                            elif response.get('id', '') == MSG_EDIT_ID:
                                await self.pubserver.send_string(json.dumps({
                                    'accountid': account.id,
                                    'type': 'edit',
                                    'data': response.get('result', {})}))
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
                # await self.repserver.send_string('echo')

                # msg['params'].update({'label': internalid})
                # self.logger.info('**** Request received:')
                # self.logger.info(msg)
                '''
                if msg['accountid'] not in orders:
                    orders[msg['accountid']] = []
                orders[msg['accountid']].append(msg)
                '''
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
            mogu = CryptoTradingAccount("mogu1988",
                                        Broker.deribit_dma, "mogu1988", "",
                                        MarketDataApi.deribit_md_websocket,
                                        TradeDataApi.deribit_td_websocket,
                                        "PmyJIl5T", "7WBI4N_YT8YB5nAFq1VjPFedLMxGfrxxCbreMFOYLv0")
            accounts.append(mogu)
            
            # create websocket for every account
            for account in accounts:
                # asyncio.create_task(self.pub_msg(account))
                asyncio.ensure_future(self.pub_msg(account))
                # fetch account info, including orders and pos
                # requests[account.id] = queue.Queue()
                # requests[account.id].put({'method': 'get_positions', 'params': {'currency': 'BTC', 'kind': 'option'}})
                # requests[account.id].put({'method': 'get_open_orders_by_currency', 'params': {'currency' : 'BTC'}})
                
            # await self.on_request()
            asyncio.ensure_future(self.on_request())


    
if __name__ == '__main__':
    service = DeribitTD('deribit-td', 'deribit-td')
    start_service(service, {})
