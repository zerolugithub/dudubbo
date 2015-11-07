import asyncio
from . import protocol
import threading
import time
from ._utils import *


class Future(object):
    FUTURES = {}
    FUTURES_LOCK = threading.Lock()

    def __init__(self, request, timeout, channel):
        self.id = request.rid
        self.timeout = timeout
        self.lock = asyncio.Lock()
        self.cond = asyncio.Condition(self.lock)
        self.reqeust = request
        self.response = None
        self.channel = channel
        self.timestamp = time.time()
        Future.FUTURES_LOCK.acquire()
        try:
            Future.FUTURES[self.id] = self
        finally:
            Future.FUTURES_LOCK.release()

    async def get(self):
        ret = await self.getWithTimeout(self.timeout)
        return ret

    async def getWithTimeout(self, timeout):
        if self.isDone():
            return self.__doReturn()
        with (await self.lock):
            if not self.isDone():
                # Coroutine wait on condition
                await self.cond.wait()
                if not self.isDone():
                    raise protocol.DubboTimeoutException('waiting response timeout. elapsed :' + str(self.timeout))

        # 拿到了对应 request id 的返回值, 调用返回handler
        return self.__doReturn()

    def setCallback(self, callback):
        pass

    def isDone(self):
        return self.response != None

    @classmethod
    async def received(cls, response):
        if response.rid in cls.FUTURES:
            Future.FUTURES_LOCK.acquire()
            try:
                if response.rid in cls.FUTURES:
                    future = Future.FUTURES[response.rid]
                    del Future.FUTURES[response.rid]
                else:
                    return
            finally:
                Future.FUTURES_LOCK.release()
            await future.doReceived(response)

    async def doReceived(self, response):
        with (await self.lock):
            self.response = response
            self.cond.notify_all()

    def __doReturn(self):
        if self.response.status != protocol.DubboResponse.OK:
            print(self.response)
            raise protocol.DubboException('DubboException : status = ' + \
                                          str(self.response.status) + ' errorMsg = ' + \
                                          str(self.response.errorMsg))
        elif self.response.exception != None:
            raise protocol.DubboException(self.response.exception)
        else:
            return self.response.result

    @classmethod
    def _checkTimeoutLoop(cls):
        try:
            for future in Future.FUTURES.values():
                if future.isDone():
                    continue
                if (time.time() - future.timestamp) > future.timeout:
                    print('find timeout future')
                    response = protocol.DubboResponse(future.id)
                    response.status = protocol.DubboResponse.SERVER_TIMEOUT
                    response.seterrorMsg = 'waiting response timeout. elapsed :' + str(future.timeout)
                    Future.received(response)
        except Exception as e:
            print('check timeout loop' + str(e))


class Endpoint(object):
    def __init__(self, addr, readHandler, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.addr = addr
        self.readHandler = readHandler

        # block until connection created
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        host, port = addr
        sock_coroutine = asyncio.open_connection(host, port)
        self.reader, self.writer = self.loop.run_until_complete(sock_coroutine)
        self.cTime = None

    def start(self):
        # new_loop = asyncio.SelectorEventLoop()
        # threading.Thread(target=self.__spawn_recv_worker, args=(new_loop,)).start()
        # future = asyncio.Future()
        future = asyncio.ensure_future(self.__recvLoop())
        # import heapq
        # heapq.heappush(self.loop._scheduled, self.__recvLoop())
        # .append()

    def __spawn_recv_worker(self, loop):
        loop.run_until_complete(self.__recvLoop())

    async def send(self, data):
        self.writer.write(data)
        # self.queue.put(data)

    # def __sendLoop(self) :
    #     while True :
    #         try :
    #             data = self.queue.get()
    #             self.writer(data)
    #             #self.sock.sendall(data)
    #         except Exception as e :
    #             print('send error, need reconnection')
    #             #self.__reconnection()

    # def __reconnection(self) :
    #     if not self.lock.acquire(False) :
    #         self.lock.acquire()
    #         self.lock.release()
    #         return
    #     try :
    #         print('start reconnection')
    #         while True :
    #             try :
    #                 print('start shutdown')
    #                 try :
    #                     self.sock.shutdown(socket.SHUT_RDWR)
    #                 except :
    #                     pass
    #                 print('finish shutdown')
    #                 del self.sock
    #                 print('create new socket')
    #                 self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #                 print('create new socket finish')
    #                 self.cTime = time.time()
    #                 print('start connect')
    #                 self.sock.connect(self.addr)
    #                 print('finish connect')
    #                 break
    #             except socket.error :
    #                 if time.time() - self.cTime < 2:
    #                     time.sleep(2)
    #     finally :
    #         self.lock.release()
    #     print('end reconnection')

    async def __reconnection(self):
        host, port = self.addr
        sock_coroutine = asyncio.open_connection(host, port)
        self.reader, self.writer = await sock_coroutine

    async def __recv(self, length):
        while True:
            data = await self.reader.read(length)
            if not data:
                print('recv error')
                await self.__reconnection()
                continue
            return data

    async def __recvLoop(self):
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

            await self.readHandler(header, data)


class DubboChannel(object):
    def __init__(self, addr):
        self.addr = addr
        self.endpoint = Endpoint(addr, self.__recvResponse)
        self.endpoint.start()
        self.lastReadTime = time.time()
        self.lastWriteTime = time.time()

    async def send(self, message):
        if isinstance(message, protocol.DubboRequest):
            data = protocol.encodeRequest(message)
        else:
            data = protocol.encodeResponse(message)

        self.lastWriteTime = time.time()
        await self.endpoint.send(data)

    async def __recvResponse(self, header, data):
        self.lastReadTime = time.time()
        obj = protocol.decode(header, data)

        if isinstance(obj, protocol.DubboRequest):
            request = obj
            if request.isHeartbeat:
                response = protocol.DubboResponse(request.rid)
                response.isEvent = True
                response.isHeartbeat = True
                await self.send(response)
            else:
                raise Exception('unimplement recv data request')
        else:
            response = obj
            if response.isHeartbeat:
                return
            await Future.received(response)
