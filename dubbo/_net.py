import asyncio
from . import protocol


class Endpoint(object):
    def __init__(self, addr):
        self.addr = addr

    async def init_connection(self):
        host, port = self.addr
        while True:
            sock_coroutine = asyncio.open_connection(host, port)
            try:
                self.reader, self.writer = await asyncio.wait_for(sock_coroutine, timeout=3)
                #print('Connected to %s:%s successfully' % self.addr)
                return
            except asyncio.TimeoutError:
                print('Tried to connect to %s:%s, timeout' % self.addr)
                sock_coroutine.close()
                continue
            except OSError as e:
                print('OSError', e)
            except Exception as e:
                print('Exception', e)

    def close_connection(self):
        try:
            self.writer.close()
            return True
        except:
            return False

    async def send(self, data):
        self.writer.write(data)

    async def __recv(self, length):
        while True:
            try:
                data = await self.reader.read(length)
            except (TimeoutError, asyncio.TimeoutError):
                print('Server %s:%s timeout, reconnect' % self.addr)
                data = None

            if not data:
                print('no data')
                self.working = False
                await self.init_connection()
                continue
            return data

    def response_handler(self, header, data):
        obj = protocol.decode(header, data)

        if obj.isHeartbeat:
            return
        return obj

    async def receive(self):
        # Wait for connection done.
        # await self._working.wait()
        while True:
            header = await self.__recv(protocol.HEADER_LENGTH)
            if header[:2] != protocol.MAGIC_NUMBER:
                continue
            while len(header) < protocol.HEADER_LENGTH:
                temp = await self.__recv(protocol.HEADER_LENGTH - len(header))
                header += temp
            dataLength = protocol.getDataLength(header)
            data = b''
            while len(data) < dataLength:
                temp = await self.__recv(dataLength - len(data))
                data += temp

            return self.response_handler(header, data)


class DubboChannel(object):
    def __init__(self, addr):
        self.addr = addr
        self.sockets = {}

    async def send_request(self, message, request_id=None, new_conn=True):
        # connected to server
        if request_id:
            if new_conn:
                # get socket from records
                endpoint = Endpoint(self.addr)
                self.sockets[request_id] = endpoint
                await endpoint.init_connection()
            else:
                endpoint = self.sockets[request_id]
        else:
            # plastic use
            endpoint = Endpoint(self.addr)
            await endpoint.init_connection()

        data = protocol.encodeRequest(message)

        # send request
        await endpoint.send(data)

        # receive response
        response = await endpoint.receive()

        # close socket
        if request_id:
            if not new_conn:
                endpoint.close_connection()
        else:
            endpoint.close_connection()
        return response.result

    def close(self, request_id):
        self.sockets[request_id].close_connection()
