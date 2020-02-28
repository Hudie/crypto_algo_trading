# -*- coding: utf-8 -*-

# from crypto_foundation.api.deribit_parser import parse_deribit_trade, parse_deribit_quote, parse_deribit_order_book, parse_deribit_instrument
from base import ServiceState, ServiceBase, start_service
import zmq.asyncio
import websockets
import json
import pickle
import time
import zlib
import asyncio



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

                lastheartbeat = time.time()
                while ws.open and self.state == ServiceState.started:
                    task = asyncio.ensure_future(ws.recv())
                    done, pending = await asyncio.wait({task}, timeout=0.0001)
                    for t in pending:
                        t.cancel()
                    response = json.loads(inflate(done.pop().result())) if done else {}

                    if response:
                        lastheartbeat = time.time()
                        if response.get('table', '') == 'option/instruments':
                            await ws.send(json.dumps({"op": "subscribe",
                                                      "args": ["option/depth5:" + i['instrument_id'] for i in response['data']]}))
                        elif response.get('table', '') == 'option/depth5':
                            # self.logger.info(response['data'])
                            self.pubserver.send_string(json.dumps(response))
                    else:
                        if time.time() - lastheartbeat > 15:
                            raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped.')
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
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
