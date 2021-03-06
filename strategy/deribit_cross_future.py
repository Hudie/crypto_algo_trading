# -*- coding: utf-8 -*-

import zmq.asyncio
import asyncio
import json
from crypto_trading.service.base import ServiceState, ServiceBase, start_service
# from crypto_trading.config import *


DERIBIT_ACCOUNT_ID = 'mogu1988'

SYMBOL = 'BTC'
MINIMUM_TICK_SIZE = 0.5
NEAR_FUTURE = 'BTC-PERPETUAL'
FAR_FUTURE = 'BTC-25SEP20'
# LONG_GAP = [115, 127, 140, 154, 170, 187, 206]
LONG_GAP = [1160, 1177, 1200, 1230, 1255]
LONG_POSITION_SIZE_THRESHOLD = [10000 * i for i in [1, 2, 3, 4, 5]]
SHORT_GAP = [245, 40, 30, 10, -20, -60, -110, -170]
SHORT_POSITION_SIZE_THRESHOLD = [250000 * i for i in [0, 0.25, 1, 2, 3, 4, 5, 6]]
SIZE_PER_TRADE = 1000


# margin: [equity, initial_margin, maintenance_margin]
margin = [0, 0, 0]
future = None
future_size = 0
perpetual = None
perpetual_size = 0


class OrderState():
    def __init__(self, if_placed=False, if_changing=False, if_cancelling=False, label='', order={}):
        self.if_placed = if_placed
        self.if_changing = if_changing
        self.if_cancelling = if_cancelling
        self.label = label
        self.order = order

    def reset(self):
        self.if_placed = False
        self.if_changing = False
        self.if_cancelling = False
        self.label = ''
        self.order = {}


f_limit_order = OrderState()
p_limit_order = OrderState()


class Quote():
    def __init__(self, bid, bidsize, ask, asksize, index_price):
        self.bid = bid
        self.bidsize = bidsize
        self.ask = ask
        self.asksize = asksize
        self.index_price = index_price


class FutureArbitrage(ServiceBase):

    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)
        # subscribe market data
        self.deribitmd = self.ctx.socket(zmq.SUB)
        self.deribitmd.connect('tcp://localhost:9050')
        self.deribitmd.setsockopt_string(zmq.SUBSCRIBE, '')
        # request client for transaction
        self.deribittdreq = self.ctx.socket(zmq.REQ)
        self.deribittdreq.connect('tcp://localhost:9020')
        # subscribe transaction data
        self.deribittd = self.ctx.socket(zmq.SUB)
        self.deribittd.connect('tcp://localhost:9010')
        self.deribittd.setsockopt_string(zmq.SUBSCRIBE, '')

        # async queue to sequentially combine market data and tx data
        self.msg = asyncio.Queue()

    # find gap between perpetual and current season future, and make transaction when conditions are satisfied
    async def find_quotes_gap(self):
        try:
            global future, future_size, f_limit_order, perpetual, perpetual_size, p_limit_order, margin

            if min(future.bid - perpetual.bid, future.ask - perpetual.ask) >= LONG_GAP[0]:
                pos_idx = sum([1 if perpetual_size >= i else 0 for i in LONG_POSITION_SIZE_THRESHOLD])
                pos_idx = min(pos_idx, len(LONG_POSITION_SIZE_THRESHOLD) - 1)
                if all((min(future.bid - perpetual.bid, future.ask - perpetual.ask) >= LONG_GAP[pos_idx],
                        perpetual_size < LONG_POSITION_SIZE_THRESHOLD[pos_idx])):
                    if not f_limit_order.if_placed:
                        await self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID, 'method': 'sell',
                            'params': {'instrument_name': FAR_FUTURE,
                                    'amount': min(SIZE_PER_TRADE, perpetual.asksize,
                                                    abs(future_size) if abs(future_size) > 0 else SIZE_PER_TRADE),
                                    'type': 'limit',
                                    'price': future.ask - MINIMUM_TICK_SIZE,
                                    'post_only': True, }
                        }))
                        f_limit_order.label = json.loads(await self.deribittdreq.recv_string())['internalid']
                        f_limit_order.if_placed = True
                    else:
                        if f_limit_order.order:
                            if not f_limit_order.order['order_state'] in ('filled', 'cancelled'):
                                if all((any((future.ask < f_limit_order.order['price'],
                                             perpetual.asksize < f_limit_order.order['amount'] - f_limit_order.order['filled_amount'])),
                                        not f_limit_order.if_changing)):
                                    self.logger.info('**** change price to: {}, future sell limit order ****'.format(future.ask - MINIMUM_TICK_SIZE))
                                    await self.deribittdreq.send_string(json.dumps({
                                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                                        'params': {'order_id': f_limit_order.order['order_id'],
                                                'amount': min(SIZE_PER_TRADE, perpetual.asksize,
                                                                abs(future_size) if abs(future_size) > 0 else SIZE_PER_TRADE),
                                                'price': future.ask - MINIMUM_TICK_SIZE,
                                                'post_only': True, }
                                    }))
                                    await self.deribittdreq.recv_string()
                                    f_limit_order.if_changing = True
                            else:
                                f_limit_order.reset()
                    if not p_limit_order.if_placed:
                        await self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy',
                            'params': {'instrument_name': NEAR_FUTURE,
                                    'amount': min(SIZE_PER_TRADE, future.bidsize,
                                                    abs(perpetual_size) if abs(perpetual_size) > 0 else SIZE_PER_TRADE),
                                    'type': 'limit',
                                    'price': perpetual.bid + MINIMUM_TICK_SIZE,
                                    'post_only': True, }
                        }))
                        p_limit_order.label = json.loads(await self.deribittdreq.recv_string())['internalid']
                        p_limit_order.if_placed = True
                    else:
                        if p_limit_order.order:
                            if not p_limit_order.order['order_state'] in ('filled', 'cancelled'):
                                if all((any((perpetual.bid > p_limit_order.order['price'],
                                             future.bidsize < p_limit_order.order['amount'] - p_limit_order.order['filled_amount'])),
                                        not p_limit_order.if_changing)):
                                    self.logger.info('**** change price to: {}, perpetual buy limit order ****'.format(perpetual.bid + MINIMUM_TICK_SIZE))
                                    await self.deribittdreq.send_string(json.dumps({
                                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                                        'params': {'order_id': p_limit_order.order['order_id'],
                                                'amount': min(SIZE_PER_TRADE, future.bidsize,
                                                                abs(perpetual_size) if abs(perpetual_size) > 0 else SIZE_PER_TRADE),
                                                'price': perpetual.bid + MINIMUM_TICK_SIZE,
                                                'post_only': True, }
                                    }))
                                    await self.deribittdreq.recv_string()
                                    p_limit_order.if_changing = True
                            else:
                                p_limit_order.reset()
            # perpetual > future situation
            elif max(future.bid - perpetual.bid, future.ask - perpetual.ask) <= SHORT_GAP[0]:
                pos_idx = sum([1 if future_size >= i else 0 for i in SHORT_POSITION_SIZE_THRESHOLD])
                pos_idx = min(pos_idx, len(SHORT_POSITION_SIZE_THRESHOLD) - 1)
                if all((max(future.bid - perpetual.bid, future.ask - perpetual.ask) <= SHORT_GAP[pos_idx],
                        future_size < SHORT_POSITION_SIZE_THRESHOLD[pos_idx])):
                    if not f_limit_order.if_placed:
                        await self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy',
                            'params': {'instrument_name': FAR_FUTURE,
                                    'amount': min(SIZE_PER_TRADE, perpetual.bidsize,
                                                    abs(future_size) if abs(future_size) > 0 else SIZE_PER_TRADE),
                                    'type': 'limit',
                                    'price': future.bid + MINIMUM_TICK_SIZE,
                                    'post_only': True, }
                        }))
                        f_limit_order.label = json.loads(await self.deribittdreq.recv_string())['internalid']
                        f_limit_order.if_placed = True
                    else:
                        if f_limit_order.order:
                            if not f_limit_order.order['order_state'] in ('filled', 'cancelled'):
                                if all((any((future.bid > f_limit_order.order['price'],
                                             perpetual.bidsize < f_limit_order.order['amount'] - f_limit_order.order['filled_amount'])),
                                        not f_limit_order.if_changing)):
                                    self.logger.info('**** change price to: {}, future buy limit order ****'.format(future.bid + MINIMUM_TICK_SIZE))
                                    await self.deribittdreq.send_string(json.dumps({
                                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                                        'params': {'order_id': f_limit_order.order['order_id'],
                                                'amount': min(SIZE_PER_TRADE, perpetual.bidsize,
                                                                abs(future_size) if abs(future_size) > 0 else SIZE_PER_TRADE),
                                                'price': future.bid + MINIMUM_TICK_SIZE,
                                                'post_only': True, }
                                    }))
                                    await self.deribittdreq.recv_string()
                                    f_limit_order.if_changing = True
                            else:
                                f_limit_order.reset()
                    if not p_limit_order.if_placed:
                        await self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID, 'method': 'sell',
                            'params': {'instrument_name': NEAR_FUTURE,
                                    'amount': min(SIZE_PER_TRADE, future.asksize,
                                                    abs(perpetual_size) if abs(perpetual_size) > 0 else SIZE_PER_TRADE),
                                    'type': 'limit',
                                    'price': perpetual.ask - MINIMUM_TICK_SIZE,
                                    'post_only': True, }
                        }))
                        p_limit_order.label = json.loads(await self.deribittdreq.recv_string())['internalid']
                        p_limit_order.if_placed = True
                    else:
                        if p_limit_order.order:
                            if not p_limit_order.order['order_state'] in ('filled', 'cancelled'):
                                if all((any((perpetual.ask < p_limit_order.order['price'],
                                             future.asksize < p_limit_order.order['amount'] - p_limit_order.order['filled_amount'])),
                                        not p_limit_order.if_changing)):
                                    self.logger.info('**** change price to: {}, perpetual sell limit order ****'.format(perpetual.ask - MINIMUM_TICK_SIZE))
                                    await self.deribittdreq.send_string(json.dumps({
                                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                                        'params': {'order_id': p_limit_order.order['order_id'],
                                                'amount': min(SIZE_PER_TRADE, future.asksize,
                                                                abs(perpetual_size) if abs(perpetual_size) > 0 else SIZE_PER_TRADE),
                                                'price': perpetual.ask - MINIMUM_TICK_SIZE,
                                                'post_only': True, }
                                    }))
                                    await self.deribittdreq.recv_string()
                                    p_limit_order.if_changing = True
                            else:
                                p_limit_order.reset()
            else:
                if f_limit_order.if_placed or p_limit_order.if_placed:
                    if not f_limit_order.if_cancelling or not p_limit_order.if_cancelling:
                        self.logger.info('**** gap disppear, cancel_all ****')
                        await self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel_all', 'params': {}
                        }))
                        await self.deribittdreq.recv_string()
                        f_limit_order.if_cancelling = True
                        p_limit_order.if_cancelling = True
        except AttributeError:
            pass
        except Exception as e:
            self.logger.exception(e)

    async def process_msg(self):
        try:
            global future, future_size, perpetual, perpetual_size, margin, f_limit_order, p_limit_order
            while self.state == ServiceState.started:
                msg = await self.msg.get()
                if msg['type'] not in ('quote', 'user.portfolio', 'buy', 'sell', 'edit'):
                    self.logger.info('---- td res: {}, {}'.format(msg['type'], msg['data']))
                if msg.get('error', ''):
                    await self.deribittdreq.send_string(json.dumps({
                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel_all', 'params': {}
                    }))
                    await self.deribittdreq.recv_string()
                    continue

                if msg['type'] == 'quote':
                    d = msg['data']
                    if d['instrument_name'] == NEAR_FUTURE:
                        perpetual = Quote(d['best_bid_price'], d['best_bid_amount'], d['best_ask_price'], d['best_ask_amount'], d['index_price'])
                    elif d['instrument_name'] == FAR_FUTURE:
                        future = Quote(d['best_bid_price'], d['best_bid_amount'], d['best_ask_price'], d['best_ask_amount'], d['index_price'])
                    await self.find_quotes_gap()
                elif msg['type'] == 'user.changes.future':
                    changes = msg['data']
                    if changes['instrument_name'] == FAR_FUTURE:
                        if changes['trades']:
                            filled = sum([tx['amount'] if tx['order_type'] == 'limit' else 0 for tx in changes['trades']])
                            if filled > 0:
                                await self.deribittdreq.send_string(json.dumps({
                                    'accountid': DERIBIT_ACCOUNT_ID,
                                    'method': 'buy' if changes['trades'][0]['direction'] == 'sell' else 'sell',
                                    'params': {'instrument_name': NEAR_FUTURE, 'amount': filled, 'type': 'market', }
                                }))
                                await self.deribittdreq.recv_string()
                        if changes['positions']:
                            future_size = changes['positions'][0]['size']
                        if changes['orders']:
                            for order in changes['orders']:
                                if order['order_type'] == 'limit' and f_limit_order.if_placed == True and f_limit_order.label == order['label']:
                                    f_limit_order.order = order
                                    f_limit_order.if_changing = False
                                    break
                    elif changes['instrument_name'] == NEAR_FUTURE:
                        if changes['trades']:
                            filled = sum([tx['amount'] if tx['order_type'] == 'limit' else 0 for tx in changes['trades']])
                            if filled > 0:
                                await self.deribittdreq.send_string(json.dumps({
                                    'accountid': DERIBIT_ACCOUNT_ID,
                                    'method': 'buy' if changes['trades'][0]['direction'] == 'sell' else 'sell',
                                    'params': {'instrument_name': FAR_FUTURE, 'amount': filled, 'type': 'market', }
                                }))
                                await self.deribittdreq.recv_string()
                        if changes['positions']:
                            perpetual_size = changes['positions'][0]['size']
                        if changes['orders']:
                            for order in changes['orders']:
                                if order['order_type'] == 'limit' and p_limit_order.if_placed == True and p_limit_order.label == order['label']:
                                    p_limit_order.order = order
                                    p_limit_order.if_changing = False
                                    break
                elif msg['type'] == 'user.portfolio':
                    portfolio = msg['data']
                    margin = [portfolio['equity'], portfolio['initial_margin'], portfolio['maintenance_margin']]
                elif msg['type'] == 'cancel_all':
                    f_limit_order.reset()
                    p_limit_order.reset()
                elif msg['type'] == 'positions':
                    for d in msg['data']:
                        if d['instrument_name'] == FAR_FUTURE:
                            future_size = d['size']
                        elif d['instrument_name'] == NEAR_FUTURE:
                            perpetual_size = d['size']
                elif msg['type'] == 'account_summary':
                    d = msg['data']
                    margin = [d['equity'], d['initial_margin'], d['maintenance_margin']]
                elif msg['type'] in ('buy', 'sell', 'edit'):
                    pass

                self.msg.task_done()
        except Exception as e:
            self.logger.exception(e)
            await self.process_msg()

    async def sub_msg_md(self):
        try:
            await asyncio.sleep(1)
            while self.state == ServiceState.started:
                task = asyncio.ensure_future(self.deribitmd.recv_string())
                done, pending = await asyncio.wait({task}, timeout=5)
                for t in pending:
                    t.cancel()
                msg = json.loads(done.pop().result()) if done else {}

                if msg:
                    if msg['type'] == 'quote':
                        await self.msg.put(msg)
                else:
                    self.logger.info('cant receive msg from future md')
                    await self.deribittdreq.send_string(json.dumps({
                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel_all', 'params': {}
                    }))
                    await self.deribittdreq.recv_string()
        except Exception as e:
            self.logger.exception(e)
            await self.sub_msg_md()

    async def sub_msg_td(self):
        try:
            await self.deribittdreq.send_string(json.dumps({
                'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_positions',
                'params': {'currency': SYMBOL, 'kind': 'future'}
            }))
            await self.deribittdreq.recv_string()
            await self.deribittdreq.send_string(json.dumps({
                'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_account_summary',
                'params': {'currency': SYMBOL, }
            }))
            await self.deribittdreq.recv_string()
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribittd.recv_string())
                if msg['accountid'] == DERIBIT_ACCOUNT_ID:
                    await self.msg.put(msg)
        except Exception as e:
            self.logger.exception(e)
            await self.sub_msg_td()

    async def balance_positions(self):
        try:
            global f_limit_order, p_limit_order
            while self.state == ServiceState.started:
                await asyncio.sleep(60)
                if not (f_limit_order.if_placed or p_limit_order.if_placed):
                    unbalanced = future_size + perpetual_size
                    if unbalanced != 0:
                        await self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID,
                            'method': 'sell' if unbalanced > 0 else 'buy',
                            'params': {'instrument_name': FAR_FUTURE if abs(future_size) > abs(perpetual_size) else NEAR_FUTURE,
                                       'amount': abs(unbalanced),
                                       'type': 'market', }
                        }))
                        await self.deribittdreq.recv_string()
        except Exception as e:
            self.logger.exception(e)
            await self.balance_positions()

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            asyncio.ensure_future(self.process_msg())
            asyncio.ensure_future(self.sub_msg_md())
            asyncio.ensure_future(self.sub_msg_td())
            asyncio.ensure_future(self.balance_positions())


if __name__ == '__main__':
    service = FutureArbitrage('cross-future')
    start_service(service, {})
