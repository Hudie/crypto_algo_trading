# -*- coding: utf-8 -*-

# from utility.enum import enum
from tornado.platform.asyncio import AsyncIOMainLoop
from crypto_trading.service.base import ServiceState, ServiceBase, start_service
import openapi_client		# deribit http rest
import logging
import zmq.asyncio
import asyncio
import tornado
import json
import pickle
import time
import random
import copy



MAX_SIZE_PER_TRADE = 10
TX_ENTRY_GAP = 45
TX_EXIT_GAP = 5

deribit_balance = [0, 0, 0]
perpetual = [0, 0, 0, 0]
future = [0, 0, 0, 0]
if_order_placed = False
if_order_canceled = False

class FutureArbitrage(ServiceBase):
    
    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)
        # sub future data
        self.deribitmd = self.ctx.socket(zmq.SUB)
        self.deribitmd.connect('tcp://localhost:9050')
        self.deribitmd.setsockopt_string(zmq.SUBSCRIBE, '')

        self.deribittd = self.ctx.socket(zmq.REQ)
        self.deribittd.connect('tcp://localhost:9020')

    # find gap between perpetual and current season future
    async def find_quotes_gap(self):
        try:
            global perpetual, future, if_order_placed, if_order_canceled
            if future[2] - perpetual[2] >= TX_ENTRY_GAP and not if_order_placed:
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988',
                    'method': 'sell',
                    'params': {'instrument_name': 'BTC-26JUN20',
                               'amount': MAX_SIZE_PER_TRADE,
                               'type': 'limit',
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True,
                    }
                }))
                msg = await self.deribittd.recv_string()
                if_order_placed = True
                if_order_canceled = False
                
                '''
                if response = filled or partially filled:
                    market_order_on_perpetual()
                elif not on_sell1:
                    change_order()
                elif gap_disppear():
                    cancel_order() and perpetual_order_if_not_all_canceled()
                '''
            elif perpetual[0] - future[0] >= TX_ENTRY_GAP and not if_order_placed:
                pass
            elif max(future[2] - perpetual[2], perpetual[0] - future[0]) < TX_ENTRY_GAP - 5 and not if_order_canceled:
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'cancel_all', 'params': {}
                }))
                msg = await self.deribittd.recv_string()
                if_order_canceled = True
                if_order_placed = False
            else:
                pass

        except Exception as e:
            self.logger.exception(e)

    # find gap, place future limit order on buy1/sell1 which may need tune on every order book change;
    # when gap disppeared, cancel order;
    # order filled or partially filled, place market order on perpetual side
    # consider the influence of left time to ENTRY point
    async def sub_msg_deribit(self):
        try:
            global deribit_balance, perpetual, future
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribitmd.recv_string())
                if msg['type'] == 'quote':
                    quote = pickle.loads(eval(msg['data']))
                    if quote['sym'] == 'BTC-PERPETUAL':
                        perpetual = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                    elif quote['sym'] == 'BTC-26JUN20':
                        future = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                    await self.find_quotes_gap()
                elif msg['type'] == 'user.portfolio':
                    portfolio = pickle.loads(eval(msg['data']))
                    deribit_balance = [portfolio['equity'], portfolio['initial_margin'], portfolio['maintenance_margin']]
                elif msg['type'] == 'user.changes.future':
                    changes = pickle.loads(eval(msg['data']))
                    self.logger.info(changes)
                    if changes['trades']:
                        future_selled = sum([tx['amount'] if tx['direction'] == 'sell' and tx['instrument_name'] == 'BTC-26JUN20' else 0
                                             for tx in changes['trades']])
                        if future_selled > 0:
                            await self.deribittd.send_string(json.dumps({
                                'accountid': 'mogu1988', 'method': 'buy',
                                'params': {'instrument_name': 'BTC-PERPETUAL', 'amount': future_selled, 'type': 'market',}
                            }))
                            msg = await self.deribittd.recv_string()
                            
                        future_bought = sum([tx['amount'] if tx['direction'] == 'buy' and tx['instrument_name'] == 'BTC-26JUN20' else 0
                                             for tx in changes['trades']])
                        if future_bought > 0:
                            await self.deribittd.send_string(json.dumps({
                                'accountid': 'mogu1988', 'method': 'sell',
                                'params': {'instrument_name': 'BTC-PERPETUAL', 'amount': future_bought, 'type': 'market',}
                            }))
                            msg = await self.deribittd.recv_string()
        except Exception as e:
            self.logger.exception(e)
                
    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            asyncio.ensure_future(self.sub_msg_deribit())

    
if __name__ == '__main__':
    service = FutureArbitrage('future-arb')
    start_service(service, {})
