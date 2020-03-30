# -*- coding: utf-8 -*-

from base import ServiceState, ServiceBase, start_service
import dateutil.parser as dp
import zmq.asyncio
import websockets
import json
import pickle
import time
import zlib
import asyncio
import requests
import hmac
import base64




# activechannels = set()
# hourlyupdated = False
api_key = '3b43d558-c6e6-4460-b8dd-1e542bc8c6c1'
secret_key = '90EB8F53AABAE20C67FB7E3B0EFBB318'
passphrase = 'mogu198812'


def get_server_time():
    url = "http://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""

def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp

def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [api_key, passphrase, timestamp, sign.decode("utf-8")]}
    login_str = json.dumps(login_param)
    return login_str

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
                timestamp = server_timestamp()
                login_str = login_params(str(timestamp), api_key, passphrase, secret_key)
                await ws.send(login_str)
                login_res = await ws.recv()
                self.logger.info('Login result: %s' % json.loads(inflate(login_res)))

                await ws.send(json.dumps({"op": "subscribe", "args": ["option/instruments:BTC-USD",
                                                                      "option/account:BTC-USD"]}))
                response = json.loads(inflate(await ws.recv()))
                #await ws.send(json.dumps({"op": "subscribe", "args": ["option/account:BTC-USD"]}))
                #response = json.loads(inflate(await ws.recv()))

                lastheartbeat = time.time()
                while ws.open and self.state == ServiceState.started:
                    task = asyncio.ensure_future(ws.recv())
                    done, pending = await asyncio.wait({task}, timeout=5)
                    for t in pending:
                        t.cancel()
                    response = json.loads(inflate(done.pop().result())) if done else {}

                    if response:
                        # self.logger.info(response)
                        lastheartbeat = time.time()
                        if response.get('table', '') == 'option/instruments':
                            self.logger.info('Instruments updated')
                            await ws.send(json.dumps({"op": "subscribe",
                                                      "args": ["option/depth5:" + i['instrument_id'] for i in response['data']]
                                                      + ["option/trade:" + i['instrument_id'] for i in response['data']]}))
                        elif response.get('table', '') in ('option/depth5', 'option/account', 'option/trade'):
                            # self.logger.info(response['data'])
                            self.pubserver.send_string(json.dumps(response))
                    else:
                        if time.time() - lastheartbeat > 30:
                            raise websockets.exceptions.ConnectionClosedError(1003, 'Serverside heartbeat stopped')
                else:
                    if self.state == ServiceState.started:
                        await self.pub_msg()
        except websockets.exceptions.ConnectionClosedError:
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
