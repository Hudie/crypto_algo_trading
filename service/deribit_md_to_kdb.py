# -*- coding: utf-8 -*-

# from utility.enum import enum
from tornado.platform.asyncio import AsyncIOMainLoop
from base import ServiceState, ServiceBase, start_service
from crypto_foundation.kdb.kdb_connection import KDBConn
from crypto_foundation.kdb.kdb_table_def import crypto_quotes, crypto_trades, crypto_instruments, deribit_order_books
import zmq.asyncio
import asyncio
import json
import pickle
import sys



KDB_HOST = sys.argv[1]
KDB_PORT = sys.argv[2]

class DeribitMDConsumer(ServiceBase):
    
    def __init__(self, sid, logger_name):
        ServiceBase.__init__(self, logger_name)

        # servie id used for control
        self.sid = sid
        
        # subscribe data from deribitmd PUB server
        self.msgclient = self.ctx.socket(zmq.SUB)
        self.msgclient.connect('tcp://localhost:9000')
        self.msgclient.setsockopt_string(zmq.SUBSCRIBE, '')

        self.msgclient2 = self.ctx.socket(zmq.SUB)
        self.msgclient2.connect('tcp://localhost:9001')
        self.msgclient2.setsockopt_string(zmq.SUBSCRIBE, '')

        self.kdb_conn = KDBConn(KDB_HOST, int(KDB_PORT), 'tickerplant', 'pass')
        self.kdb_conn.open()
        
    # deal with data source 1
    async def sub_msg(self):
        type_dict = {'quote': crypto_quotes, 'trade': crypto_trades,
                     'book': deribit_order_books, 'instrument': crypto_instruments}
        print('Begin message consuming')
        while self.state == ServiceState.started:
            msg = json.loads(await self.msgclient.recv_string())
            # deal with the coming msg
            if self.kdb_conn.is_connected():
                # print(pickle.loads(eval(msg['data'])))
                self.kdb_conn.pub(type_dict[msg['type']],
                                  [pickle.loads(eval(msg['data']))], is_tickerplant = True)
            else:
                self.kdb_conn.close()
                self.kdb_conn.open()

    # deal with data source 2
    async def sub_msg2(self):
        while self.state == ServiceState.started:
            msg = await self.msgclient2.recv_string()
            # deal with the coming msg
            print(msg)

    async def run(self):
        if self.state == ServiceState.started:
            self.logger.error('tried to run service, but state is %s' % self.state)
        else:
            self.state = ServiceState.started
            # run sub_msg & sub_msg2 concurrently
            await asyncio.gather(self.sub_msg(), self.sub_msg2())


    
if __name__ == '__main__':
    service = DeribitMDConsumer('deribit-md-to-kdb', 'deribit-md-to-kdb')
    start_service(service, {})
