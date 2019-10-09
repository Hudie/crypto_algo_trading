from tornado.platform.asyncio import AsyncIOMainLoop
from tornado import web
import tornado
import asyncio
import zmq.asyncio
import json



ctx = zmq.asyncio.Context()
repserver = ctx.socket(zmq.REP)
repserver.bind('tcp://*:8810')
pubserver = ctx.socket(zmq.PUB)
pubserver.bind('tcp://*:8820')

service_node_status = {}


async def on_request():
    try:
        while True:
           msg = json.loads(await repserver.recv_string())
           await repserver.send_string('copy')
           service_node_status[msg['sid']] = msg
           print(service_node_status)
    except Exception as e:
        print(e)

class ControlHandler(tornado.web.RequestHandler):
    async def post(self):
        sid, action = (self.get_argument(i, '') for i in ('sid', 'action'))
        pubserver.send_string(json.dumps({'sid': sid, 'action': action}))
        self.write('Msg pub')

application = web.Application([
    (r"/control", ControlHandler),
])


if __name__ == "__main__":
    AsyncIOMainLoop().install()
    application.listen(8888)
    loop = tornado.ioloop.IOLoop.current()
    loop.spawn_callback(on_request)
    loop.start()
