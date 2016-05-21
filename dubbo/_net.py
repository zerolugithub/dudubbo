import asyncio
import socket
from . import protocol


class Endpoint(object):
    def __init__(self, addr):
        self.addr = addr

    def init_connection(self):
        host, port = self.addr
        while True:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.settimeout(1)

            try:
                self.sock.connect((host, port))
                #self.reader, self.writer = asyncio.wait_for(sock_coroutine, timeout=1)
                print('Connected to %s:%s successfully' % self.addr)
                return
            except Exception as e:
                print('Tried to connect to %s:%s, error' % self.addr)
                raise e

    def close_connection(self):
        try:
            self.sock.close()
            return True
        except:
            return False

    def send(self, data):
        self.sock.send(data)

    def __recv(self, length):
        while True:
            try:
                data = self.sock.recv(length)
            except (TimeoutError):
                print('Server %s:%s timeout, reconnect' % self.addr)
                data = None

            if not data:
                print('no data')
                self.working = False
                self.init_connection()
                continue
            return data

    def response_handler(self, header, data):
        obj = protocol.decode(header, data)

        if obj.isHeartbeat:
            return
        return obj

    def receive(self):
        # Wait for connection done.
        # self._working.wait()
        while True:
            header = self.__recv(protocol.HEADER_LENGTH)
            if header[:2] != protocol.MAGIC_NUMBER:
                continue
            while len(header) < protocol.HEADER_LENGTH:
                temp = self.__recv(protocol.HEADER_LENGTH - len(header))
                header += temp
            dataLength = protocol.getDataLength(header)
            data = b''
            while len(data) < dataLength:
                temp = self.__recv(dataLength - len(data))
                data += temp

            return self.response_handler(header, data)


class DubboChannel(object):
    def __init__(self, addr):
        self.addr = addr
        self.sockets = {}

    def send_request(self, message, request_id=None, new_conn=True):
        # connected to server
        if request_id:
            if new_conn:
                # get socket from records
                endpoint = Endpoint(self.addr)
                self.sockets[request_id] = endpoint
                endpoint.init_connection()
            else:
                endpoint = self.sockets[request_id]
        else:
            # plastic use
            endpoint = Endpoint(self.addr)
            endpoint.init_connection()

        data = protocol.encodeRequest(message)

        # send request
        endpoint.send(data)

        # receive response
        response = endpoint.receive()

        # close socket
        if request_id:
            if not new_conn:
                endpoint.close_connection()
        else:
            endpoint.close_connection()
        return response.result

    def close(self, request_id):
        self.sockets[request_id].close_connection()
