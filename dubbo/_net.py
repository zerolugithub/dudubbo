import asyncio

from dubbo.log import logger
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
            logger.error('DubboException:', str(self.response.status), self.response.errorMsg)
            raise protocol.DubboException('DubboException : status = ' + \
                                          str(self.response.status) + ' errorMsg = ' + \
                                          str(self.response.errorMsg))
        elif self.response.exception != None:
            logger.error('DubboException: ' + self.response.exception)
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
            logger.error('Future check timeout loop' + str(e))
            print('check timeout loop' + str(e))


class Endpoint(object):
    def __init__(self, addr, readHandler, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.addr = addr
        self.readHandler = readHandler
        self.cTime = None
        self._lock = asyncio.Lock()
        self._working = asyncio.Event()
        self.working = False

    async def init_connection(self):
        host, port = self.addr
        while True:
            sock_coroutine = asyncio.open_connection(host, port)
            try:
                self.reader, self.writer = await asyncio.wait_for(sock_coroutine, timeout=3)
                logger.info('Connected to %s:%s successfully' % self.addr)
                #print('Connected to %s:%s successfully' % self.addr)
                self.working = True
                with (await self._lock):
                    self._working.set()
                break
            except asyncio.TimeoutError:
                logger.error('Tried to connect to %s:%s, timeout' % self.addr)
                #print('Tried to connect to %s:%s, timeout' % self.addr)
                sock_coroutine.close()
                continue
            except OSError as e:
                logger.error('OSError: %s(%s:%s)' % (str(e),) + self.addr)
                #print('OSError', e)
            except Exception as e:
                logger.error('Exception: %s(%s:%s)' % (str(e),) + self.addr)
                #print('Exception', e)

    def start(self):
        # new_loop = asyncio.SelectorEventLoop()
        # threading.Thread(target=self.__spawn_recv_worker, args=(new_loop,)).start()
        # future = asyncio.Future()
        # 开始接收循环, 考虑直接丢当前的IOLoop还是spawn一个新thread
        future = asyncio.wait([self.init_connection(), self.__recvLoop()])
        asyncio.ensure_future(future)
        #asyncio.ensure_future(self.init_connection())
        #asyncio.ensure_future(self.__recvLoop())
        #asyncio.wait(self.init_connection(), 3)
        #asyncio.ensure_future(self.__recvLoop())

    def __spawn_recv_worker(self, loop):
        loop.run_until_complete(self.__recvLoop())

    async def send(self, data):
        #if self.working:
        await self._working.wait()
        self.writer.write(data)

    async def __reconnection(self):
        print('Lost remote connection %s:%s, try to reconnect' % self.addr)
        host, port = self.addr
        sock_coroutine = asyncio.open_connection(host, port)
        self.reader, self.writer = await sock_coroutine
        print('Connected %s:%s' % self.addr)


    async def __recv(self, length):
        while True:
            try:
                data = await self.reader.read(length)
            except (TimeoutError, asyncio.TimeoutError):
                logger.error('Connection to %s:%s, timeout' % self.addr)
                #print('Server %s:%s timeout, reconnect')
                data = None


            if not data:
                logger.error('Recv empty (%s:%s)' % self.addr)
                self.working = False
                #await self.__reconnection()
                await self.init_connection()
                continue
            return data

    async def __recvLoop(self):
        # Wait for connection done.
        await self._working.wait()
        logger.info('Connection established: %s:%s' % self.addr)
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
