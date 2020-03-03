# -*- coding: utf-8 -*-

# from utility.enum import enum
from tornado.platform.asyncio import AsyncIOMainLoop
from crypto_trading.service.base import ServiceState, ServiceBase, start_service
import logging
import zmq.asyncio
import asyncio
import tornado
import json
import pickle
import time



QUOTE_GAP = 0.0045
WITHIN_SECONDS = 15 * 24 * 3600

quotes = {}


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

    async def find_quotes_gap(self):
        for k, v in quotes.items():
            tmp = k.split('-')
            if all((not v.get('gapped', False),
                    time.mktime(time.strptime(tmp[1], '%d%b%y')) - time.time() < WITHIN_SECONDS,
                    tmp[-1] == 'C' and float(tmp[2]) > float(v.get('index_price', 0)) + 400, )):
                    # OTM
                    # any((tmp[-1] == 'C' and float(tmp[2]) > float(v.get('index_price', 0)) + 400,
                    #     tmp[-1] == 'P' and float(tmp[2]) < float(v.get('index_price', 0)) - 400, )), )):
                if 'deribit' in v.keys() and 'okex' in v.keys():
                    if v['deribit'][0] and v['okex'][0]:
                        if float(v['okex'][0]) - v['deribit'][0] >= QUOTE_GAP:
                            self.logger.info(k + ' -- ' + str(v))
                            v['gapped'] = True
                            asyncio.ensure_future(self.gap_transaction())
                    if v['deribit'][2] and v['okex'][2]:
                        if v['deribit'][2] - float(v['okex'][2]) >= QUOTE_GAP:
                            self.logger.info(k + ' -- ' + str(v))
                            v['gapped'] = True
                            asyncio.ensure_future(self.gap_transaction())

    async def gap_transaction(self):
        # need make transaction at okex firstly, and then at deribit only after it returns successfully
        # make sure which side price is beyond the mark price, trade it first
        # within N days, prefer OTM call first
        pass

    async def sub_msg_deribit(self):
        while self.state == ServiceState.started:
            msg = json.loads(await self.deribitmsgclient.recv_string())
            if msg['type'] == 'quote':
                quote = pickle.loads(eval(msg['data']))
                # print(quote)
                newrecord = [quote['bid_prices'][0], quote['bid_sizes'][0], quote['ask_prices'][0], quote['ask_sizes'][0]]
                if quotes.setdefault(quote['sym'], {}).get('deribit', []) != newrecord:
                    quotes[quote['sym']].update({'deribit': newrecord, 'gapped': False,
                                                 'index_price': quote['index_price'], 'mark_price': quote['mark_price']})
                    await self.find_quotes_gap()

    async def sub_msg_okex(self):
        while self.state == ServiceState.started:
            msg = json.loads(await self.okexmsgclient.recv_string())
            if msg['table'] == 'option/depth5':
                quote = msg['data'][0]
                # print(quote)
                tmp = quote['instrument_id'].split('-')
                sym = '-'.join([tmp[0], time.strftime('%d%b%y', time.strptime(tmp[2], '%y%m%d')).upper(),
                                tmp[3], tmp[4]])
                newrecord = [quote['bids'][0][0] if len(quote['bids']) > 0 else None,
                             quote['bids'][0][1] if len(quote['bids']) > 0 else None,
                             quote['asks'][0][0] if len(quote['asks']) > 0 else None,
                             quote['asks'][0][1] if len(quote['asks']) > 0 else None]
                if quotes.setdefault(sym, {}).get('okex', []) != newrecord:
                    quotes[sym].update({'okex': newrecord, 'gapped': False})
                    await self.find_quotes_gap()
                
    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            print('Here in run body')
            self.state = ServiceState.started
            # await self.sub_msg()
            asyncio.ensure_future(self.sub_msg_deribit())
            asyncio.ensure_future(self.sub_msg_okex())

    
if __name__ == '__main__':
    service = CatchGap('catch_gap_onsameside')
    start_service(service, {})
