# -*- coding: utf-8 -*-

from crypto_trading.service.base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import json
import pickle
from crypto_trading.config import *



deribit_margin = [0, 0, 0]
perpetual = []
future = []
future_size = 0
perpetual_size = 0
can_place_order = True
if_order_cancelling = False
if_price_changing = False
current_order = {}
current_order_idx = 0


class FutureArbitrage(ServiceBase):
    
    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)
        # sub future data
        self.deribitmd = self.ctx.socket(zmq.SUB)
        self.deribitmd.connect('tcp://localhost:9050')
        self.deribitmd.setsockopt_string(zmq.SUBSCRIBE, '')

        self.deribittdreq = self.ctx.socket(zmq.REQ)
        self.deribittdreq.connect('tcp://localhost:9020')

        self.deribittd = self.ctx.socket(zmq.SUB)
        self.deribittd.connect('tcp://localhost:9010')
        self.deribittd.setsockopt_string(zmq.SUBSCRIBE, '')

    # find gap between perpetual and current season future, and make transaction when conditions are satisfied
    async def find_quotes_gap(self):
        try:
            global perpetual, future, can_place_order, if_order_cancelling, if_price_changing, current_order, future_size
            global current_order_idx

            gap = max(future[2] - perpetual[2], perpetual[0] - future[0])
            gap_idx = max(sum([1 if gap >= i else 0 for i in TX_ENTRY_GAP]) - 1, 0)
            can_entry = False if max(abs(future_size), abs(perpetual_size)) >= POSITION_SIZE_THRESHOLD[gap_idx] else True
            can_exit = False if min(abs(future_size), abs(perpetual_size)) <= 100 else True

            #self.logger.info('gap_idx: {}, can_place_order: {}, if_order_cancelling: {}, if_price_changing: {}'.format(
            #    gap_idx, can_place_order, if_order_cancelling, if_price_changing))
            
            # future > perpetual entry point
            if future[2] - perpetual[2] >= TX_ENTRY_GAP[gap_idx] and can_place_order and can_entry:
                self.logger.info(' **** entry order gap: {}'.format(future[2] - perpetual[2]))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'sell',
                    'params': {'instrument_name': SEASON_FUTURE,
                               'amount': min(SIZE_PER_TRADE, perpetual[3]),
                               'type': 'limit',
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                current_order_idx = gap_idx
                can_place_order = False
            # future > perpetual entry point: change limit order price
            elif all((perpetual[2] + TX_ENTRY_GAP[max(gap_idx, current_order_idx)] - TX_ENTRY_GAP_CANCEL_DELTA <= future[2] < current_order.get('price', 0),
                      not if_price_changing,
                      current_order)):
                self.logger.info('---- entry change price to: {} ----'.format(max(future[2] - 0.5, future[0] + 0.5)))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': min(SIZE_PER_TRADE, perpetual[3]),
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                if_price_changing = True
            # perpetual > future entry point
            elif perpetual[0] - future[0] >= TX_ENTRY_GAP[gap_idx] and can_place_order and can_entry:
                self.logger.info(' **** entry order gap: {}'.format(future[0] - perpetual[0]))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy',
                    'params': {'instrument_name': SEASON_FUTURE,
                               'amount': min(SIZE_PER_TRADE, perpetual[1]),
                               'type': 'limit',
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                current_order_idx = gap_idx
                can_place_order = False
            # perpetual > future entry point: change limit order price
            elif all((perpetual[0] - TX_ENTRY_GAP[max(gap_idx, current_order_idx)] + TX_ENTRY_GAP_CANCEL_DELTA >= future[0] > current_order.get('price', 0),
                      not if_price_changing,
                      current_order)):
                self.logger.info('---- entry change price to: {} ----'.format(min(future[0] + 0.5, future[2] - 0.5)))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': min(SIZE_PER_TRADE, perpetual[1]),
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                if_price_changing = True
            # cancel orders in this area
            elif all((max(future[2] - perpetual[2], perpetual[0] - future[0]) < TX_ENTRY_GAP[max(gap_idx, current_order_idx)] - TX_ENTRY_GAP_CANCEL_DELTA,
                      max(future[0] - perpetual[0], perpetual[2] - future[2]) > TX_EXIT_GAP_CANCEL,
                      not can_place_order,
                      not if_order_cancelling,
                      current_order.get('order_state', '') not in ('filled', 'cancelled', ''))):
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel', 'params': {'order_id': current_order['order_id']}
                }))
                self.deribittdreq.recv_string()
                if_order_cancelling = True
                current_order_idx = 0
            # future > perpetual exit point
            elif future[0] - perpetual[0] <= TX_EXIT_GAP and future_size < 0 and can_exit and can_place_order:
                self.logger.info(' **** exit order gap: {}'.format(future[0] - perpetual[0]))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy',
                    'params': {'instrument_name': SEASON_FUTURE,
                               'amount': min(SIZE_PER_TRADE, abs(future_size), perpetual[1]),
                               'type': 'limit',
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                can_place_order = False
            # future > perpetual exit point: change limit order price
            elif current_order.get('price', 999999) < future[0] <= perpetual[0] + TX_EXIT_GAP_CANCEL and not if_price_changing:
                self.logger.info('---- exit change price to: {} ----'.format(min(future[0] + 0.5, future[2] - 0.5)))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': min(SIZE_PER_TRADE, abs(future_size), perpetual[1]),
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                if_price_changing = True
            # perpetual > future exit point
            elif perpetual[2] - future[2] <= TX_EXIT_GAP and future_size > 0 and can_exit and can_place_order:
                self.logger.info(' **** exit order gap: {}'.format(perpetual[2] - future[2]))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'sell',
                    'params': {'instrument_name': SEASON_FUTURE,
                               'amount': min(SIZE_PER_TRADE, abs(future_size), perpetual[3]),
                               'type': 'limit',
                               'price': max(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                can_place_order = False
            # perpetual > future exit point: change limit order price
            elif current_order.get('price', 0) > future[2] >= perpetual[2] - TX_EXIT_GAP_CANCEL and not if_price_changing:
                self.logger.info('---- exit change price to: {} ----'.format(max(future[0] + 0.5, future[2] - 0.5)))
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': min(SIZE_PER_TRADE, abs(future_size), perpetual[3]),
                               'price': max(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                self.deribittdreq.recv_string()
                if_price_changing = True
            else:
                pass

        except IndexError:
            pass
        except Exception as e:
            self.logger.exception(e)

    # find gap, place future limit order on buy1/sell1 which may need tune on every order book change;
    # when gap disppeared, cancel order;
    # order filled or partially filled, place market order on perpetual side
    # consider the influence of left time to ENTRY point
    async def sub_msg_md(self):
        try:
            global deribit_margin, perpetual, future, can_place_order, if_order_cancelling, if_price_changing
            global current_order, future_size, perpetual_size

            # await asyncio.sleep(1)
            while self.state == ServiceState.started:
                task = asyncio.ensure_future(self.deribitmd.recv_string())
                done, pending = await asyncio.wait({task}, timeout=5)
                for t in pending:
                    t.cancel()
                msg = json.loads(done.pop().result()) if done else {}
                    
                # msg = json.loads(await self.deribitmd.recv_string())
                if msg:
                    if msg['type'] == 'quote':
                        quote = pickle.loads(eval(msg['data']))
                        # self.logger.info('++++ quote: {}'.format(quote))
                        if quote['sym'] == 'BTC-PERPETUAL':
                            perpetual = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                        elif quote['sym'] == SEASON_FUTURE:
                            future = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                        await self.find_quotes_gap()
                else:
                    self.logger.info('cant receive msg from future md')
                    self.deribittdreq.send_string(json.dumps({
                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel_all', 'params': {}
                    }))
                    self.deribittdreq.recv_string()
                    if_order_cancelling = True
        except Exception as e:
            self.logger.exception(e)
            await self.sub_msg_md()

    async def sub_msg_td(self):
        try:
            global deribit_margin, perpetual, future, can_place_order, if_order_cancelling, if_price_changing
            global current_order, future_size, perpetual_size
            
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribittd.recv_string())
                if msg['accountid'] != DERIBIT_ACCOUNT_ID:
                    continue
                
                if msg['type'] == 'positions':
                    data = msg['data']
                    for d in data:
                        if d['instrument_name'] == SEASON_FUTURE:
                            future_size = d['size']
                        elif d['instrument_name'] == 'BTC-PERPETUAL':
                            perpetual_size = d['size']
                    if abs(future_size + perpetual_size) > SIZE_PER_TRADE and can_place_order:
                        instrument_name, method = '', ''
                        if future_size < 0 and future_size + perpetual_size > 0:
                            instrument_name, method = 'BTC-PERPETUAL', 'sell'
                        elif future_size > 0 and future_size + perpetual_size > 0:
                            instrument_name, method = SEASON_FUTURE, 'sell'
                        elif future_size < 0 and future_size + perpetual_size < 0:
                            instrument_name, method = SEASON_FUTURE, 'buy'
                        elif future_size > 0 and future_size + perpetual_size < 0:
                            instrument_name, method = 'BTC-PERPETUAL', 'buy'
                        if instrument_name and method:
                            self.deribittdreq.send_string(json.dumps({
                                'accountid': DERIBIT_ACCOUNT_ID, 'method': method,
                                'params': {'instrument_name': instrument_name,
                                           'amount': abs(future_size + perpetual_size) - SIZE_PER_TRADE,
                                           'type': 'market',}
                            }))
                            self.deribittdreq.recv_string()
                elif msg['type'] in ('buy', 'sell',):
                    self.logger.info('#### td res: {}: {}'.format(msg['type'], msg['data']))
                    if msg['data']:
                        data = msg['data']
                        if data['order']['instrument_name'] == SEASON_FUTURE and data['order']['order_type'] == 'limit':
                            current_order = data['order']
                        elif data['order']['instrument_name'] == 'BTC-PERPETUAL' and current_order:
                            self.deribittdreq.send_string(json.dumps({
                                'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_order_state',
                                'params': {'order_id': current_order['order_id']}
                            }))
                            self.deribittdreq.recv_string()
                        else:
                            pass
                elif msg['type'] == 'order_state':
                    if msg['data']['order_state'] in ('filled', 'cancelled'):
                        current_order = {}
                        can_place_order = True
                        if_order_cancelling = False
                        if_price_changing = False
                    else:
                        current_order = msg['data']
                elif msg['type'] == 'open_orders':
                    if sum([1 if (i['instrument_name'] == SEASON_FUTURE and i['order_type'] == 'limit') else 0 for i in msg['data']]) > 1:
                        self.deribittdreq.send_string(json.dumps({
                            'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel_all', 'params': {}
                        }))
                        self.deribittdreq.recv_string()
                        if_order_cancelling = True
                elif msg['type'] == 'edit':
                    if_price_changing = False
                    if msg['data']:
                        current_order = msg['data']['order']
                elif msg['type'] in ('cancel', 'cancel_all'):
                    self.logger.info('#### td res: {}: {}'.format(msg['type'], msg['data']))
                    current_order = {}
                    if_order_cancelling = False
                elif msg['type'] == 'user.portfolio':
                    portfolio = msg['data']
                    deribit_margin = [portfolio['equity'], portfolio['initial_margin'], portfolio['maintenance_margin']]
                elif msg['type'] == 'user.changes.future':
                    changes = msg['data']
                    if changes['instrument_name'] == SEASON_FUTURE:
                        if changes['trades']:
                            future_filled = sum([tx['amount'] if tx['order_type'] == 'limit' else 0 for tx in changes['trades']])
                            self.deribittdreq.send_string(json.dumps({
                                'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy' if changes['trades'][0]['direction'] == 'sell' else 'sell',
                                'params': {'instrument_name': 'BTC-PERPETUAL', 'amount': future_filled, 'type': 'market',}
                            }))
                            self.deribittdreq.recv_string()
                        '''
                        if all([True if o['order_state'] in ('filled', 'cancelled') else False for o in changes['orders']]) or not changes['orders']:
                            can_place_order = True
                            current_order = {}
                        else:
                            current_order = changes['orders'][0]
                        '''
                        if changes['positions']:
                            future_size = changes['positions'][0]['size']
                    elif changes['instrument_name'] == 'BTC-PERPETUAL':
                        if changes['positions']:
                            perpetual_size = changes['positions'][0]['size']
                else:
                    pass
        except Exception as e:
            self.logger.exception(e)
            await self.sub_msg_td()

    async def get_current_order_state(self):
        try:
            global current_order
            while self.state == ServiceState.started:
                if current_order.get('order_state', '') not in ('filled', 'cancelled', ''):
                    self.deribittdreq.send_string(json.dumps({
                        'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_order_state',
                        'params': {'order_id': current_order['order_id']}
                    }))
                    self.deribittdreq.recv_string()
                await asyncio.sleep(1)
        except Exception as e:
            self.logger.exception(e)
            await self.get_current_order_state()

    async def get_current_positions_and_orders(self):
        try:
            while self.state == ServiceState.started:
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_positions',
                    'params': {'currency': 'BTC', 'kind': 'future'}
                }))
                self.deribittdreq.recv_string()
                await asyncio.sleep(1)
                '''
                self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_open_orders_by_currency',
                    'params': {'currency': 'BTC', 'kind': 'future'}
                }))
                self.deribittdreq.recv_string()
                '''
        except Exception as e:
            self.logger.exception(e)
            await self.get_current_positions_and_orders()
            
    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            asyncio.ensure_future(self.sub_msg_md())
            asyncio.ensure_future(self.sub_msg_td())
            asyncio.ensure_future(self.get_current_order_state())
            asyncio.ensure_future(self.get_current_positions_and_orders())
            
    
if __name__ == '__main__':
    service = FutureArbitrage('future-arb')
    start_service(service, {})
