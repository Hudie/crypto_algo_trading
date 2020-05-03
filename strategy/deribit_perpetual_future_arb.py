# -*- coding: utf-8 -*-

from crypto_trading.service.base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import asyncio
import json
import pickle



MAX_SIZE_PER_TRADE = 10
TX_ENTRY_GAP = 40
TX_ENTRY_GAP_CANCEL = 38
TX_EXIT_GAP = 5
POSITION_SIZE_THRESHOLD = 82300

deribit_balance = [0, 0, 0]
perpetual = [0, 0, 0, 0]
future = [0, 0, 0, 0]
can_place_order = True
stop_strategy = False


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
            global perpetual, future, can_place_order
            if future[2] - perpetual[2] >= TX_ENTRY_GAP and can_place_order:
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
                
            elif perpetual[0] - future[0] >= TX_ENTRY_GAP and can_place_order:
                pass
            elif max(future[2] - perpetual[2], perpetual[0] - future[0]) < TX_ENTRY_GAP_CANCEL and not can_place_order:
                await self.deribittd.send_string(json.dumps({
                    'accountid': 'mogu1988', 'method': 'cancel_all', 'params': {}
                }))
                msg = await self.deribittd.recv_string()

        except Exception as e:
            self.logger.exception(e)

    # find gap, place future limit order on buy1/sell1 which may need tune on every order book change;
    # when gap disppeared, cancel order;
    # order filled or partially filled, place market order on perpetual side
    # consider the influence of left time to ENTRY point
    async def sub_msg_deribit(self):
        try:
            global deribit_balance, perpetual, future, can_place_order
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribitmd.recv_string())
                if msg['type'] == 'quote' and not stop_strategy:
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
                        if all([True if o['order_state'] == 'filled' else False for o in changes['orders']]) or not changes['orders']:
                            can_place_order = True
                        else:
                            can_place_order = False
                        if changes['positions']:
                            if abs(changes['positions'][0]['size']) >= POSITION_SIZE_THRESHOLD:
                                stop_strategy = True
                            
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
