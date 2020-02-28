# -*- coding: utf-8 -*-

# from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json
import pickle
import time
import zlib



# activechannels = set()
# hourlyupdated = False


def inflate(data):
    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


class OkexMD(ServiceBase):
    
    def __init__(self, sid, logger_name):
        ServiceBase.__init__(self, logger_name)

        self.sid = sid
        # zmq PUB socket for publishing market data
        self.pubserver = self.ctx.socket(zmq.PUB)
        self.pubserver.bind('tcp://*:9100')

    async def pub_msg(self):
        # get marketdata from exchange socket, then pub to zmq
        try:
            async with websockets.connect('wss://real.OKEx.com:8443/ws/v3') as ws:
                self.logger.info('Connected to okex websocket server')

                await ws.send(json.dumps({"op": "subscribe", "args": ["option/instruments:BTC-USD"]}))
                response = json.loads(inflate(await ws.recv()))
                # self.logger.info(response)
                
                while ws.open and self.state == ServiceState.started:
                    response = json.loads(inflate(await ws.recv()))
                    # self.logger.info(response)
                    if response.get('table', '') == 'option/instruments':
                        await ws.send(json.dumps({"op": "subscribe",
                                                  "args": ["option/depth5:" + i['instrument_id'] for i in response['data']]}))
                    elif response.get('table', '') == 'option/depth5':
                        # self.logger.info(response['data'])
                        self.pubserver.send_string(json.dumps(response))
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
                    
                '''
                global activechannels, hourlyupdated
                # get instruments and then update channels
                await websocket.send(json.dumps(instruments))
                response = json.loads(await websocket.recv())
                for i in response['result']:
                    self.pubserver.send_string(json.dumps({'type': 'instrument',
                                                           'data': str(pickle.dumps(parse_deribit_instrument(i)))}))
                    for j in ('trades', 'ticker', 'book'):
                        activechannels.add('.'.join([j, i['instrument_name'], 'raw']))
                subscribe['params']['channels'] = list(activechannels)
                await websocket.send(json.dumps(subscribe))
                hourlyupdated = True

                # it is very important here to use 'self.state' to control start/stop!!!
                lastheartbeat = time.time()
                while websocket.open and self.state == ServiceState.started:
                    # check heartbeat to see if websocket is broken
                    if time.time() - lastheartbeat > 30:
                        raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped.')
                    
                    # update instruments every hour
                    if time.gmtime().tm_min == 5 and hourlyupdated == False:
                        self.logger.info('Fetching instruments hourly ******')
                        await websocket.send(json.dumps(instruments))
                        hourlyupdated = True
                    elif time.gmtime().tm_min == 31 and hourlyupdated == True:
                        hourlyupdated = False
                    else:
                        pass
                    
                    response = json.loads(await websocket.recv())
                    # need response heartbeat to keep alive
                    if response.get('id', '') == MSG_INSTRUMENTS_ID:
                        newchannels = set()
                        for i in response['result']:
                            for j in ('trades', 'ticker', 'book'):
                                newchannels.add('.'.join([j, i['instrument_name'], 'raw']))
                        if len(newchannels.difference(activechannels)) > 0:
                            self.logger.info('There are new channels as following:')
                            self.logger.info(str(newchannels.difference(activechannels)))
                            subscribe['params']['channels'] = list(newchannels)
                            await websocket.send(json.dumps(subscribe))
                            unsubscribe['params']['channels'] = list(activechannels.difference(newchannels))
                            await websocket.send(json.dumps(unsubscribe))
                            newinstruments = set()
                            for i in newchannels.difference(activechannels):
                                newinstruments.add(i.split('.')[1])
                            for i in response['result']:
                                if i['instrument_name'] in newinstruments:
                                    self.pubserver.send_string(json.dumps({'type': 'instrument',
                                                                           'data': str(pickle.dumps(parse_deribit_instrument(i)))}))
                            activechannels = newchannels
                    elif response.get('id', '') in (MSG_TEST_ID, MSG_SUBSCRIBE_ID, MSG_UNSUBSCRIBE_ID):
                        pass
                    else:
                        # self.logger.info(str(response['params']['data']))
                        if response['params']['channel'].startswith('trades'):
                            for i in response['params']['data']:
                                self.pubserver.send_string(json.dumps({'type': 'trade',
                                                                       'data': str(pickle.dumps(parse_deribit_trade(i)))}))
                        elif response['params']['channel'].startswith('ticker'):
                            self.pubserver.send_string(
                                json.dumps({'type': 'quote',
                                            'data': str(pickle.dumps(parse_deribit_quote(response['params']['data'])))}))
                        elif response['params']['channel'].startswith('book'):
                            self.pubserver.send_string(
                                json.dumps({'type': 'book',
                                            'data': str(pickle.dumps(parse_deribit_order_book(response['params']['data'])))}))
                        else:
                            pass
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
                '''
        except Exception as e:
            self.logger.exception(e)
            await self.pub_msg()

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            await self.pub_msg()


    
if __name__ == '__main__':
    service = OkexMD('okex-md', 'okex-md')
    start_service(service, {'port': 9100, 'ip': 'localhost'})
