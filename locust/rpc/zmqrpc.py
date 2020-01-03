import zmq.green as zmq
from zmq import ZMQError
from .protocol import Message
from locust.util.exception_handler import retry

class BaseSocket(object):
    def __init__(self, sock_type):
        context = zmq.Context()
        self.socket = context.socket(sock_type)

        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)

    def bind_handler(self, handler):
        self.handler = handler

    @retry()
    def send_all(self, msg):
        self.socket.send('all ' + msg.serialize())

    @retry()
    def send_to(self, id, msg):
        self.socket.send(id + ' ' + msg.serialize())

    @retry()
    def send_to_client(self, msg):
        self.socket.send_multipart([msg.node_id.encode(), msg.serialize()])

    @retry()
    def recv(self):
        data = self.socket.recv()
        msg = Message.unserialize(data.split(' ', 1)[1])
        getattr(self.handler, 'on_' + msg.type)(msg)

    @retry()
    def recv_from_client(self):
        data = self.socket.recv_multipart()
        addr = data[0].decode()
        msg = Message.unserialize(data[1])
        return addr, msg

    @retry()
    def close(self):
        self.socket.close()
        self.context.term()

class MasterServer(BaseSocket):
    def __init__(self, host, port):
        context = zmq.Context()
        self.context = context

        self.socket = context.socket(zmq.SUB)
        self.socket.bind("tcp://%s:%i" % (host, port))
        self.socket.setsockopt(zmq.SUBSCRIBE, '')

        self.socket = context.socket(zmq.PUB)
        self.socket.bind("tcp://%s:%i" % (host, port + 1))


class SlaveClient(BaseSocket):
    def __init__(self, host, port, id):
        context = zmq.Context()
        self.context = context

        self.socket = context.socket(zmq.SUB)
        self.socket.connect("tcp://%s:%i" % (host, port + 1))
        self.socket.setsockopt(zmq.SUBSCRIBE, 'all')
        self.socket.setsockopt(zmq.SUBSCRIBE, id)

        self.socket = context.socket(zmq.PUB)
        self.socket.connect("tcp://%s:%i" % (host, port))


class SlaveServer(MasterServer):
    def __init__(self, port):
        super(SlaveServer, self).__init__('*', port + 2)


class WorkerClient(SlaveClient):
    def __init__(self, port, id):
        super(WorkerClient, self).__init__('127.0.0.1', port + 2, id)
