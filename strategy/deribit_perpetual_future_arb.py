# -*- coding: utf-8 -*-

from crypto_trading.service.base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import json
import pickle



MAX_SIZE_PER_TRADE = 10
TX_ENTRY_GAP = 60
TX_ENTRY_GAP_CANCEL = 56
TX_EXIT_GAP = 10
TX_EXIT_GAP_CANCEL = 13
POSITION_SIZE_THRESHOLD = [100, 120000]

deribit_balance = [0, 0, 0]
perpetual = [0, 0, 0, 0]
future = [0, 0, 0, 0]
future_size = -82600
can_place_order = True
if_order_cancelling = False
if_price_changing = False
stop_entry = False
stop_exit = False
current_order = {}


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
            global perpetual, future, can_place_order, if_order_cancelling, if_price_changing, current_order, future_size
            global stop_entry, stop_exit
            # future > perpetual entry point
            if future[2] - perpetual[2] >= TX_ENTRY_GAP and can_place_order and not stop_entry:
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'sell',
                    'params': {'instrument_name': 'BTC-26JUN20',
                               'amount': MAX_SIZE_PER_TRADE,
                               'type': 'limit',
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True, }
                }))
                msg = await self.deribittd.recv_string()
                can_place_order = False
            # future > perpetual entry point: change limit order price
            elif perpetual[2] + TX_ENTRY_GAP_CANCEL <= future[2] < current_order.get('price', 0) and not if_price_changing:
                self.logger.info('=============== change limit price: {}'.format(future[2]))
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': current_order['amount'] - current_order['filled_amount'],
                               'price': max(future[2] - 0.5, future[0] + 0.5),
                               'post_only': True, }
                }))
                msg = await self.deribittd.recv_string()
                if_price_changing = True
            # perpetual > future entry point
            elif perpetual[0] - future[0] >= TX_ENTRY_GAP and can_place_order and not stop_entry:
                pass
            # cancel orders in this area
            elif all((max(future[2] - perpetual[2], perpetual[0] - future[0]) < TX_ENTRY_GAP_CANCEL,
                      max(future[0] - perpetual[0], perpetual[2] - future[2]) > TX_EXIT_GAP_CANCEL,
                      not can_place_order,
                      not if_order_cancelling)):
                self.logger.info('***************** cancel order')
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'cancel_all', 'params': {}
                }))
                msg = await self.deribittd.recv_string()
                if_order_cancelling = True
            # future > perpetual exit point
            elif future[0] - perpetual[0] <= TX_EXIT_GAP and future_size < 0 and not stop_exit and can_place_order:
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'buy',
                    'params': {'instrument_name': 'BTC-26JUN20',
                               'amount': MAX_SIZE_PER_TRADE,
                               'type': 'limit',
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                msg = await self.deribittd.recv_string()
                can_place_order = False
            # future > perpetual exit point: change limit order price
            elif current_order.get('price', 999999) < future[0] <= perpetual[0] + TX_EXIT_GAP_CANCEL and not if_price_changing:
                self.logger.info('=============== change limit price: {}'.format(future[0]))
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'edit',
                    'params': {'order_id': current_order['order_id'],
                               'amount': current_order['amount'] - current_order['filled_amount'],
                               'price': min(future[0] + 0.5, future[2] - 0.5),
                               'post_only': True, }
                }))
                msg = await self.deribittd.recv_string()
                if_price_changing = True
            # perpetual > future exit point
            elif perpetual[2] - future[2] <= TX_EXIT_GAP and future_size > 0 and not stop_exit:
                pass

        except Exception as e:
            self.logger.exception(e)

    # find gap, place future limit order on buy1/sell1 which may need tune on every order book change;
    # when gap disppeared, cancel order;
    # order filled or partially filled, place market order on perpetual side
    # consider the influence of left time to ENTRY point
    async def sub_msg_deribit(self):
        try:
            global deribit_balance, perpetual, future, can_place_order, if_order_cancelling, if_price_changing
            global stop_entry, stop_exit, current_order, future_size
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
                    if changes['instrument_name'] == 'BTC-26JUN20':
                        if changes['trades']:
                            future_filled = sum([tx['amount'] for tx in changes['trades']])
                            await self.deribittd.send_string(json.dumps({
                                'accountid': 'mogu1988', 'method': 'buy' if changes['trades'][0]['direction'] == 'sell' else 'sell',
                                'params': {'instrument_name': 'BTC-PERPETUAL', 'amount': future_filled, 'type': 'market',}
                            }))
                            msg = await self.deribittd.recv_string()
                        if all([True if o['order_state'] in ('filled', 'cancelled') else False for o in changes['orders']]) or not changes['orders']:
                            can_place_order = True
                            if_order_cancelling = False
                            current_order = {}
                        else:
                            can_place_order = False
                            current_order = changes['orders'][0]
                            if_price_changing = False
                        if changes['positions']:
                            future_size = changes['positions'][0]['size']
                            if abs(future_size) >= POSITION_SIZE_THRESHOLD[1]:
                                stop_entry = True
                            elif abs(future_size) <= POSITION_SIZE_THRESHOLD[0]:
                                stop_exit = True
                    if changes['instrument_name'] == 'BTC-PERPETUAL':
                        perpetual_size = changes['positions'][0]['size']
                        if abs(perpetual_size) >= POSITION_SIZE_THRESHOLD[1]:
                            stop_entry = True
                        elif abs(perpetual_size) <= POSITION_SIZE_THRESHOLD[0]:
                            stop_exit = True
                            
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
