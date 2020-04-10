# -*- coding: utf-8 -*-

# from utility.enum import enum
from tornado.platform.asyncio import AsyncIOMainLoop
from crypto_trading.service.base import ServiceState, ServiceBase, start_service
from okex.option_api import OptionAPI
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



RISK_RATIO = 2
MAX_SIZE_PER_TRADE = 3
'''
QUOTE_GAP = (
    (0.003, 0.36, 3 * 24 * 3600),
    (0.0045, 0.33, 6 * 24 * 3600),
    (0.006, 0.32, 10 * 24 * 3600),
    (0.0075, 0.31, 15 * 24 *3600),
    (0.01, 0.3, 31 * 24 * 3600),
)
'''
QUOTE_GAP = (
    (0.0025, 0.36, 3 * 24 * 3600),
    (0.004, 0.33, 6 * 24 * 3600),
    (0.0055, 0.32, 10 * 24 * 3600),
    (0.007, 0.31, 15 * 24 *3600),
    (0.009, 0.3, 31 * 24 * 3600),
)

deribit_apikey = 'PmyJIl5T'
deribit_apisecret = '7WBI4N_YT8YB5nAFq1VjPFedLMxGfrxxCbreMFOYLv0'

quotes = {}
locked_size = 0
deribit_balance = [0, 0, 0]	# equity, initial_margin, maintenance_margin
okex_balance = [0, 0, 0]


class CatchGap(ServiceBase):
    
    def __init__(self, logger_name):
        ServiceBase.__init__(self, logger_name)

        # servie id used for control
        # self.sid = 'sid002'
        
        # SUB data
        self.deribitmsgclient = self.ctx.socket(zmq.SUB)
        self.deribitmsgclient.connect('tcp://localhost:9000')
        self.deribitmsgclient.setsockopt_string(zmq.SUBSCRIBE, '')

        self.okexmsgclient = self.ctx.socket(zmq.SUB)
        self.okexmsgclient.connect('tcp://localhost:9100')
        self.okexmsgclient.setsockopt_string(zmq.SUBSCRIBE, '')

        self.okexclient = OptionAPI('3b43d558-c6e6-4460-b8dd-1e542bc8c6c1',
                                    '90EB8F53AABAE20C67FB7E3B0EFBB318', 'mogu198812', True)

        self.deribitauth = openapi_client.AuthenticationApi()
        res = self.deribitauth.public_auth_get(grant_type='client_credentials',
                                               username='', password='',
                                               client_id=deribit_apikey, client_secret=deribit_apisecret,
                                               refresh_token='', timestamp='', signature='')
        self.deribitconfig = openapi_client.Configuration()
        self.deribitconfig.access_token = res['result']['access_token']
        self.deribitconfig.refresh_token = res['result']['refresh_token']
        self.deribittradingapi = openapi_client.TradingApi(openapi_client.ApiClient(self.deribitconfig))

    async def refresh_token(self):
        while True:
            await asyncio.sleep(800)
            res = self.deribitauth.public_auth_get(grant_type='refresh_token',
                                                   username='', password='',
                                                   client_id='', client_secret='',
                                                   refresh_token=self.deribitconfig.refresh_token,
                                                   timestamp='', signature='')
            self.deribitconfig.access_token = res['result']['access_token']
            self.deribitconfig.refresh_token = res['result']['refresh_token']
            self.deribittradingapi = openapi_client.TradingApi(openapi_client.ApiClient(self.deribitconfig))
            # self.logger.info('deribit token refreshed')

    async def find_quotes_gap(self, sym):
        try:
            global deribit_balance, okex_balance
            v = quotes[sym]
            if all(( not v.get('gapped', False),
                     not v.get('trading', False),
                     sym[-1] == 'C',
            )):
                timedelta = time.mktime(time.strptime(sym.split('-')[1], '%d%b%y')) - time.time()
                delta = abs(v.get('delta', 1))
                if 'deribit' in v.keys() and 'okex' in v.keys():
                    if v['deribit'][0] and v['okex'][2]:
                        for (gap, d, t) in QUOTE_GAP:
                            if v['deribit'][0] - float(v['okex'][2]) >= gap and timedelta <= t and delta <= d:
                                self.logger.info('----------------------------------------------------------')
                                self.logger.info('%s -- gap: %.4f -- %s' %(sym, v['deribit'][0] - float(v['okex'][2]), str(v)))
                                self.logger.info('deribit: %s, okex: %s' %(str(deribit_balance), str(okex_balance)))
                                v.update({'gapped': True, 'trading': True})
                                asyncio.ensure_future(self.gap_trade(sym, copy.copy(v), False))
                                break
                    if v['deribit'][2] and v['okex'][0]:
                        for (gap, d, t) in QUOTE_GAP:
                            if float(v['okex'][0]) - v['deribit'][2] >= gap and timedelta <= t and delta <= d:
                                self.logger.info('----------------------------------------------------------')
                                self.logger.info('%s -- gap: %.4f -- %s' %(sym, float(v['okex'][0]) - v['deribit'][2], str(v)))
                                self.logger.info('deribit: %s, okex: %s' %(str(deribit_balance), str(okex_balance)))
                                v.update({'gapped': True, 'trading': True})
                                asyncio.ensure_future(self.gap_trade(sym, copy.copy(v), True))
                                break
        except Exception as e:
            self.logger.exception(e)

    async def gap_trade(self, sym, quote, if_okex_sell):
        # make sure both platforms can trade
        # calculate how many contracts need to be traded concerning sizes and account situation
        # trade at one platform and make sure that it returns successfully, if not, how to deal with it?
        # trade at the other platform
        # check everything alright
        try:
            global locked_size, deribit_balance, okex_balance

            if if_okex_sell:
                size = min( min(
                    float(quote['okex'][1])/10,
                    quote['deribit'][3],
                    int(max(0, okex_balance[0]/RISK_RATIO-okex_balance[2], okex_balance[0]-okex_balance[1])/0.15*10)/10.0,
                    int(max(0, deribit_balance[0]-deribit_balance[2]*RISK_RATIO, deribit_balance[0]-deribit_balance[1])/quote['deribit'][2]*10)/10.0,
                ) - locked_size, MAX_SIZE_PER_TRADE)
                
                # long firstly, then short
                if size >= 0.1:
                    locked_size += size
                    self.logger.info('trade size: %f' % size)
                    res = self.deribittradingapi.private_buy_get(sym, size, price=quote['deribit'][2], time_in_force='immediate_or_cancel')
                    self.logger.info(res['result'])
                    filled_qty = res['result']['order']['filled_amount']
                    self.logger.info('filled quantity: %.1f' % filled_qty)
                    locked_size -= size - filled_qty
                    for price in (float(quote['okex'][0])-i*0.0005 for i in range(int((float(quote['okex'][0])-quote['deribit'][2])/0.0005))):
                        if filled_qty > 0:
                            ret = self.okexclient.order({'instrument_id': quote['oksym'],
                                                         'side': 'sell',
                                                         'price': str(price),
                                                         'size': str(int(filled_qty * 10)), })
                            self.logger.info('okex sell at price %.4f with size %.1f' % (price, filled_qty))
                            self.logger.info(ret)
                            if ret['error_code'] == '0' and ret['result'] == 'true':
                                # await asyncio.sleep(1)
                                order_id = ret['order_id']
                                order_status = self.okexclient.get_order_status(order_id)
                                self.logger.info(order_status)
                                try:
                                    if order_status['state'] != '2':	# means not filled completely
                                        self.okexclient.cancel_order(order_id)
                                        order_status = self.okexclient.get_order_status(order_id)
                                except Exception as e:
                                    self.logger.info('failed to cancel order')
                                    order_status = self.okexclient.get_order_status(order_id)
                                filled_qty = filled_qty - float(order_status.get('filled_qty', 0))/10
                        else:
                            break
            else:
                size = min( min(
                    float(quote['okex'][3])/10,
                    quote['deribit'][1],
                    int(max(0, okex_balance[0]-okex_balance[2]*RISK_RATIO, okex_balance[0]-okex_balance[1])/float(quote['okex'][2])*10)/10.0,
                    int(max(0, deribit_balance[0]/RISK_RATIO-deribit_balance[2], deribit_balance[0]-deribit_balance[1])/0.15*10)/10.0,
                ) - locked_size, MAX_SIZE_PER_TRADE)

                if size >= 0.1:
                    locked_size += size
                    self.logger.info('trade size: %f' % size)
                    ret = self.okexclient.order({'instrument_id': quote['oksym'],
                                                 'side': 'buy',
                                                 'price': quote['okex'][2],
                                                 'size': str(int(size * 10)),
                    })
                    self.logger.info(ret)
                    
                    if not (ret['error_code'] == '0' and ret['result'] == 'true'):
                        locked_size -= size
                    else:
                        order_id = ret['order_id']
                        order_status = self.okexclient.get_order_status(order_id)
                        self.logger.info(order_status)
                        try:
                            if order_status['state'] != '2':	# means not filled completely
                                self.okexclient.cancel_order(order_id)
                                order_status = self.okexclient.get_order_status(order_id)
                        except Exception as e:
                            # self.logger.exception(e)
                            self.logger.info('failed to cancel order')
                            order_status = self.okexclient.get_order_status(order_id)
                        filled_qty = float(order_status.get('filled_qty', 0))/10
                        self.logger.info('filled quantity: %.1f' % filled_qty)
                        locked_size -= size - filled_qty
                        
                        if filled_qty > 0:
                            res = self.deribittradingapi.private_sell_get(sym, filled_qty, price=quote['deribit'][0], time_in_force='immediate_or_cancel')
                            self.logger.info(res['result'])
                            order = res['result']['order']
                            while order['filled_amount'] < order['amount']:
                                if order['price'] - 0.0005 >= float(quote['okex'][2]):
                                    res = self.deribittradingapi.private_sell_get(sym, order['amount']-order['filled_amount'],
                                                                                  price=order['price']-0.0005, time_in_force='immediate_or_cancel')
                                    order = res['result']['order']
                                    self.logger.info(res['result'])
                                else:
                                    break
            quote['trading'] = False
        except Exception as e:
            self.logger.exception(e)

    async def sub_msg_deribit(self):
        try:
            global deribit_balance, okex_balance
            while self.state == ServiceState.started:
                msg = json.loads(await self.deribitmsgclient.recv_string())
                if msg['type'] == 'quote':
                    quote = pickle.loads(eval(msg['data']))
                    newrecord = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                    if quotes.setdefault(quote['sym'], {}).get('deribit', []) != newrecord:
                        quotes[quote['sym']].update({'deribit': newrecord, 'gapped': False, 'index_price': quote['index_price'],
                                                     'mark_price': quote['mark_price'], 'delta': quote['delta'], })
                        await self.find_quotes_gap(quote['sym'])
                elif msg['type'] == 'user.portfolio':
                    portfolio = pickle.loads(eval(msg['data']))
                    deribit_balance = [portfolio['equity'], portfolio['initial_margin'], portfolio['maintenance_margin']]
        except Exception as e:
            self.logger.exception(e)

    async def sub_msg_okex(self):
        try:
            global deribit_balance, okex_balance
            while self.state == ServiceState.started:
                msg = json.loads(await self.okexmsgclient.recv_string())
                if msg['table'] == 'option/depth5':
                    quote = msg['data'][0]
                    tmp = quote['instrument_id'].split('-')
                    sym = '-'.join([tmp[0], time.strftime('%d%b%y', time.strptime(tmp[2], '%y%m%d')).upper(),
                                    tmp[3], tmp[4]])
                    newrecord = [quote['bids'][0][0] if len(quote['bids']) > 0 else None,
                                 quote['bids'][0][1] if len(quote['bids']) > 0 else None,
                                 quote['asks'][0][0] if len(quote['asks']) > 0 else None,
                                 quote['asks'][0][1] if len(quote['asks']) > 0 else None]
                    if quotes.setdefault(sym, {}).get('okex', []) != newrecord:
                        quotes[sym].update({'okex': newrecord, 'gapped': False, 'oksym': quote['instrument_id'], })
                        await self.find_quotes_gap(sym)
                elif msg['table'] == 'option/account':
                    accountinfo = msg['data'][0]
                    # self.logger.info(accountinfo)
                    okex_balance = [float(accountinfo['margin_balance']),
                                    float(accountinfo['margin_for_unfilled']) + float(accountinfo['margin_frozen']),
                                    float(accountinfo['maintenance_margin'])]
                    if random.randint(0, 99) == 0:
                        self.logger.info('deribit: %s, okex: %s' %(str(deribit_balance), str(okex_balance)) )
        except Exception as e:
            self.logger.exception(e)
                
    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            # await self.sub_msg()
            asyncio.ensure_future(self.sub_msg_deribit())
            asyncio.ensure_future(self.sub_msg_okex())
            asyncio.ensure_future(self.refresh_token())

    
if __name__ == '__main__':
    service = CatchGap('catch-gap')
    start_service(service, {})
