
import threading
import struct
from . import hessian2

HEADER_LENGTH = 16
MAGIC_NUMBER = b'\xda\xbb'
FLAG_REQUEST = 0x80
FLAG_TWOWAY = 0x40
FLAG_EVENT = 0x20
HESSIAN2_CONTENT_TYPE_ID = 2

DOUBLE_VERSION = '2.3.3'

RESPONSE_NULL_VALUE = 2
RESPONSE_VALUE = 1
RESPONSE_WITH_EXCEPTION = 0

class RpcInvocation(object) :
    def __init__(self, methodName = None, paramTypes = None, params = None, attachments = None) :
        self.methodName = methodName
        self.paramTypes = paramTypes or ''
        self.params = params or []
        self.attachments = attachments or {}

class RpcResult(object) :
    def __init__(self, value = None, exception = None) :
        self.value = value
        self.exception = exception

class DubboRequest(object) :
    ridLock = threading.Lock()
    nextRid = 0

    def __init__(self, rid=None, twoWay=True, event=False, broken=False, data=None):

        if rid == None:
            with self.ridLock:
                self.rid = DubboRequest.nextRid
                DubboRequest.nextRid += 1
        else :
            self.rid = rid

        self.isTwoWay = twoWay
        self.isEvent = event
        self.isBroken = broken
        self.data = data
        self.isHeartbeat = False

    def __str__(self) :
        return 'DubboRequest :' + str(self.__dict__)

class DubboResponse(object) :
    OK = 20
    CLIENT_TIMEOUT = 30
    SERVER_TIMEOUT = 31
    BAD_REQUEST = 40
    BAD_RESPONSE = 50
    SERVICE_NOT_FOUND = 60
    SERVICE_ERROR = 70
    SERVER_ERROR = 80
    CLIENT_ERROR = 90

    def __init__(self, rid):
        self.rid = rid
        self.status = DubboResponse.OK
        self.isEvent = False
        self.isHeartbeat = False
        self.version = ''
        self.errorMsg = ''
        self.result = None
        self.exception = None

    def isHeartBeat(self):
        return self.event and self.result == None

    def setEvent(self, result):
        self.event = True
        self.result = result

    def __str__(self):
        return 'DubboResponse :' + str(self.__dict__)


class DubboException(Exception):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        for i in self.data.stackTrace:
            print(''.join([(str(x[0]) + ':' + str(x[1]) + '\n') for x in i.__dict__.items() if not x[0].startswith('_')]))
        return 'DubboException :' + str(self.data)


class DubboTimeoutException(Exception):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'DubboTimeoutException :', self.data

def encodeRequestData(invocation) :
    out = hessian2.Hessian2Output()
    out.writeObject(DOUBLE_VERSION)
    out.writeObject(invocation.attachments['path'])
    out.writeObject(invocation.attachments['version'])
    out.writeObject(invocation.methodName)
    out.writeObject(invocation.paramTypes)
    for param in invocation.params:
        #oo = hessian2.Hessian2Output()
        #oo.writeObject(param)
        #hessian2.printByteStr(oo.getByteString())
        out.writeObject(param)
    out.writeObject(invocation.attachments)
    return out.getByteString()


def encodeEventData(data):
    out = hessian2.Hessian2Output()
    out.writeObject(data)
    return out.getByteString()


def encodeRequest(request):
    if not isinstance(request, DubboRequest) :
        raise TypeError('encodeRequest only support DubboRequest type')
    header = b''
    header += MAGIC_NUMBER
    flag = HESSIAN2_CONTENT_TYPE_ID | FLAG_REQUEST
    if request.isEvent:
        flag |= FLAG_EVENT
    if request.isTwoWay:
        flag |= FLAG_TWOWAY

    header += flag.to_bytes(1, 'big')
    header += b'\x00'
    header += struct.pack('>q', request.rid)
    print(request.rid)

    if request.isEvent:
        pass
        data = encodeEventData(request.data)
    else :
        data = encodeRequestData(request.data)

    dataLength = len(data)
    header += struct.pack('>i', dataLength)

    return header + data


def encodeResponseData(rpcResult):
    if rpcResult.exception:
        out.write(RESPONSE_WITH_EXCEPTION)
        out.write(rpcResult.value)
    elif rpcResult.value == None:
        out.write(RESPONSE_NULL_VALUE)
    else:
        out.write(RESPONSE_VALUE)
        out.write(rpcResult.value)


def encodeResponse(response):
    if not isinstance(response, DubboResponse):
        raise TypeError('encodeResponse only support DubboResponse type')
    header = b''
    header += MAGIC_NUMBER
    flag = HESSIAN2_CONTENT_TYPE_ID
    if response.isEvent:
        flag |= FLAG_EVENT
    header += flag.to_bytes(1, 'big')
    header += response.status.to_bytes(1, 'big')
    header += struct.pack('>q', response.rid)

    if response.status == DubboResponse.OK:
        if response.isEvent:
            data = encodeEventData(response.result)
        else:
            data = encodeResponseData(response.result)

    dataLength = len(data)
    header += struct.pack('>i', dataLength)

    return header + data


def getDataLength(header):
    return struct.unpack('>i', header[12:])[0]


def getRequestId(header):
    return struct.unpack('>q', header[4:12])[0]


def decodeResponseData(response, input):
    flag = input.readObject()
    if flag == RESPONSE_NULL_VALUE:
        response.result = None
    elif flag == RESPONSE_VALUE:
        response.result = input.readObject()
    elif flag == RESPONSE_WITH_EXCEPTION:
        response.exception = input.readObject()


def decodeRequestData(request, input):
    return None

def decode(header, data):
    flag = header[2]
    status = header[3]
    rid = getRequestId(header)
    input = hessian2.Hessian2Input(data)
    if flag & FLAG_REQUEST != 0:
        request = DubboRequest(rid=rid)
        request.isTwoWay = flag & FLAG_TWOWAY != 0
        if flag & FLAG_EVENT != 0:
            request.isEvent = True
            request.isHeartbeat = True
        if request.isEvent:
            request.data = input.readObject()
        else:
            decodeRequestData(request, input)
        return request
    else:
        response = DubboResponse(rid)
        response.status = status
        response.isEvent = flag & FLAG_EVENT
        if response.status != DubboResponse.OK:
            response.errorMsg = input.readObject()
        else:
            if response.isEvent:
                response.isHeartbeat = True
                response.result = input.readObject()
            else:
                decodeResponseData(response, input)
        return response

if __name__ == '__main__':
    request = DubboRequest()
    data = encodeRequest(request)
    hessian2.printByteStr(data)
