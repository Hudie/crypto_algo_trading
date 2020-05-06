# -*- coding: utf-8 -*-

from crypto_trading.service.base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import json
import pickle



DERIBIT_ACCOUNT_ID = 'mogu1988'
SEASON_FUTURE = 'BTC-26JUN20'

SIZE_PER_TRADE = 500
TX_ENTRY_GAP = [40, 50, 60, 70, 80, 90, 100]
TX_ENTRY_GAP_CANCEL_DELTA = 2
TX_EXIT_GAP = 6
TX_EXIT_GAP_CANCEL = TX_EXIT_GAP + 2
POSITION_SIZE_THRESHOLD = [150000 * i for i in [1, 2, 3, 4, 5, 6, 7]]

deribit_margin = [0, 0, 0]
perpetual = []
future = []
future_size = 0
perpetual_size = 0
can_place_order = True
if_order_cancelling = False
if_price_changing = False
current_order = {}


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

            # self.logger.info('--- future size: {} & perpetual size: {} ---'.format(future_size, perpetual_size))
            gap = max(future[2] - perpetual[2], perpetual[0] - future[0])
            gap_idx = max(sum([1 if gap >= i else 0 for i in TX_ENTRY_GAP]) - 1, 0)
            can_entry = False if max(abs(future_size), abs(perpetual_size)) >= POSITION_SIZE_THRESHOLD[gap_idx] else True
            can_exit = False if min(abs(future_size), abs(perpetual_size)) <= 100 else True
            
            # future > perpetual entry point
            if future[2] - perpetual[2] >= TX_ENTRY_GAP[gap_idx] and can_place_order and can_entry:
                await self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'sell',
                    'params': {'instrument_name': SEASON_FUTURE,
                               'amount': min(SIZE_PER_TRADE, perpetual[3]),
                               'type': 'limit',
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True, }
                }))
                await self.deribittdreq.recv_string()
                can_place_order = False
            # future > perpetual entry point: change limit order price
            elif all((perpetual[2] + TX_ENTRY_GAP[gap_idx] - TX_ENTRY_GAP_CANCEL_DELTA <= future[2] < current_order.get('price', 0),
                      not if_price_changing)):
                self.logger.info('---- change price to: {} ----'.format(max(future[2] - 0.5, future[0] + 0.5)))
                await self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': min(SIZE_PER_TRADE, perpetual[3]),
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True, }
                }))
                await self.deribittdreq.recv_string()
                if_price_changing = True
            # perpetual > future entry point
            elif perpetual[0] - future[0] >= TX_ENTRY_GAP[gap_idx] and can_place_order and can_entry:
                pass
            # cancel orders in this area
            elif all((max(future[2] - perpetual[2], perpetual[0] - future[0]) < TX_ENTRY_GAP[gap_idx] - TX_ENTRY_GAP_CANCEL_DELTA,
                      max(future[0] - perpetual[0], perpetual[2] - future[2]) > TX_EXIT_GAP_CANCEL,
                      not can_place_order,
                      not if_order_cancelling)):
                await self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'cancel_all', 'params': {}
                }))
                await self.deribittdreq.recv_string()
                if_order_cancelling = True
            # future > perpetual exit point
            elif future[0] - perpetual[0] <= TX_EXIT_GAP and future_size < 0 and can_exit and can_place_order:
                await self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy',
                    'params': {'instrument_name': SEASON_FUTURE,
                               'amount': min(SIZE_PER_TRADE, abs(future_size), perpetual[1]),
                               'type': 'limit',
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                await self.deribittdreq.recv_string()
                can_place_order = False
            # future > perpetual exit point: change limit order price
            elif current_order.get('price', 999999) < future[0] <= perpetual[0] + TX_EXIT_GAP_CANCEL and not if_price_changing:
                await self.deribittdreq.send_string(json.dumps({
                    'accountid': DERIBIT_ACCOUNT_ID, 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': min(SIZE_PER_TRADE, abs(future_size), perpetual[1]),
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                await self.deribittdreq.recv_string()
                if_price_changing = True
            # perpetual > future exit point
            elif perpetual[2] - future[2] <= TX_EXIT_GAP and future_size > 0 and can_exit:
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
            
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribitmd.recv_string())
                if msg['type'] == 'quote':
                    quote = pickle.loads(eval(msg['data']))
                    if quote['sym'] == 'BTC-PERPETUAL':
                        perpetual = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                    elif quote['sym'] == SEASON_FUTURE:
                        future = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                    await self.find_quotes_gap()
                elif msg['type'] == 'user.portfolio':
                    portfolio = pickle.loads(eval(msg['data']))
                    deribit_margin = [portfolio['equity'], portfolio['initial_margin'], portfolio['maintenance_margin']]
                elif msg['type'] == 'user.changes.future':
                    changes = pickle.loads(eval(msg['data']))
                    self.logger.info(changes)
                    if changes['instrument_name'] == SEASON_FUTURE:
                        if changes['trades']:
                            future_filled = sum([tx['amount'] for tx in changes['trades']])
                            await self.deribittdreq.send_string(json.dumps({
                                'accountid': DERIBIT_ACCOUNT_ID, 'method': 'buy' if changes['trades'][0]['direction'] == 'sell' else 'sell',
                                'params': {'instrument_name': 'BTC-PERPETUAL', 'amount': future_filled, 'type': 'market',}
                            }))
                            await self.deribittdreq.recv_string()
                        if all([True if o['order_state'] in ('filled', 'cancelled') else False for o in changes['orders']]) or not changes['orders']:
                            can_place_order = True
                            if_order_cancelling = False
                            current_order = {}
                        else:
                            can_place_order = False
                            if_price_changing = False
                            current_order = changes['orders'][0]
                        if changes['positions']:
                            future_size = changes['positions'][0]['size']
                    elif changes['instrument_name'] == 'BTC-PERPETUAL':
                        if changes['positions']:
                            perpetual_size = changes['positions'][0]['size']

        except Exception as e:
            self.logger.exception(e)

    async def sub_msg_td(self):
        try:
            global future_size, perpetual_size

            await self.deribittdreq.send_string(json.dumps({
                'accountid': DERIBIT_ACCOUNT_ID, 'method': 'get_positions',
                'params': {'currency': 'BTC', 'kind': 'future'}
            }))
            await self.deribittdreq.recv_string()
            
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribittd.recv_string())
                if msg['type'] == 'positions':
                    data = msg['data']
                    for d in data:
                        if d['instrument_name'] == SEASON_FUTURE:
                            future_size = d['size']
                        elif d['instrument_name'] == 'BTC-PERPETUAL':
                            perpetual_size = d['size']
        except Exception as e:
            self.logger.exception(e)
                
    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            asyncio.ensure_future(self.sub_msg_md())
            asyncio.ensure_future(self.sub_msg_td())

    
if __name__ == '__main__':
    service = FutureArbitrage('future-arb')
    start_service(service, {})
