# coding=utf-8
import inspect
import json
import uuid

from . import protocol
from . import hessian2
import threading
import types
import json
import datetime
import time
import random
from . import java
from .constants import *
from . import _model
from ._net import *

__version__ = '0.1.1'


def _getRequestParam(request, key, default=None):
    if key in request.data.attachments:
        return request.data.attachments[key]
    return default


class DubboClient(object):
    def __init__(self, addrs, config, enable_heartbeat=False):
        self.channels = []
        self._enable_heartbeat = enable_heartbeat
        for addr in addrs:
            self.channels.append(DubboChannel(addr))

        if config and KEY_HEARTBEAT in config:
            self.heartbeat = config[KEY_HEARTBEAT]
        else:
            self.heartbeat = DEFAULT_HEARTBEAT



        # for long connection
        self.long_conn_records = {}

    def close_channel(self, request_id):
        channel = self.long_conn_records[request_id]
        channel.close(request_id)

    def invoke(self, rpcInvocation, request_id=None):
        request = protocol.DubboRequest()
        request.data = rpcInvocation

        if request_id and self.long_conn_records.get(request_id):
            # get registered channel
            channel = self.long_conn_records[request_id]
        else:
            # set new channel
            channel = self.__selectChannel(request)
            self.long_conn_records[request_id] = channel

        timeout = _getRequestParam(request, KEY_TIMEOUT)
        withReturn = _getRequestParam(request, KEY_WITH_RETURN, True)
        is_= _getRequestParam(request, KEY_ASYNC, False)

        if not withReturn:
            channel.send(request)
            return
        else:
            if request_id:
                # For long connection
                if request_id in channel.sockets:
                    ret = channel.send_request(request, request_id=request_id, new_conn=False)
                else:
                    ret = channel.send_request(request, request_id=request_id)
            else:
                # For short connection
                ret = channel.send_request(request)
            return ret

    def __selectChannel(self, request):
        index = random.randint(0, len(self.channels) - 1)
        return self.channels[index]


class ServiceProxy(object):
    def __init__(self, client, classInfo, attachments):
        self.client = client
        self.classInfo = classInfo
        self.attachments = attachments
        self.long_conn = False
        if KEY_METHOD in attachments:
            self.methodConfig = attachments[KEY_METHOD]
            del attachments[KEY_METHOD]
        else:
            self.methodConfig = {}

    def __enter__(self):
        self.long_conn = True
        self.request_id = str(uuid.uuid4())
        # create a dummy channel
        self.client.long_conn_records[self.request_id] = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close_channel(self.request_id)
        del self.client.long_conn_records[self.request_id]
        self.long_conn = False

    def _updateConfig(self, config):
        config = config.copy()
        if KEY_METHOD in config:
            methodConfig = config[KEY_METHOD]
            del config[KEY_METHOD]
        else:
            methodConfig = None
        self.attachments.update(config)
        if methodConfig:
            for methodName, methodConfig in methodConfig.items():
                if methodName not in self.methodConfig:
                    self.methodConfig[methodName] = methodConfig.copy()
                else:
                    self.methodConfig[methodName].update(methodConfig)

    def invoke(self, name, args):
        if type(name) == str:
            name = name.encode('utf-8')
        if not name in self.classInfo.methodMap:
            raise KeyError('interface ' + self.classInfo.thisClass + ' has no method name ' + str(name))
        methods = self.classInfo.methodMap[name]
        if len(methods) > 1:
            method = self.__guessMethod(methods, args)
            if method == None:
                errorStr = 'can not find match method : ' + \
                           self.classInfo.thisClass + '.' + name + '(' + \
                           ', '.join([type(arg) for arg in args]) + \
                           ') maybe : '
                for method in methods:
                    errorStr += self.classInfo.thisClass + '.' + name + '(' + \
                                ', '.join(java.analyseParamTypes(self.__getParamType(method))) + \
                                ')'
                raise KeyError(errorStr)
        else:
            method = methods[0]

        paramType = self.__getParamType(method)

        attachments = self.attachments.copy()
        if name in self.methodConfig:
            attachments.update(self.methodConfig[name])
        invocation = protocol.RpcInvocation(name, paramType, args, attachments)
        if not self.request_id in self.client.long_conn_records:
            return (self.client.invoke(invocation))
        else:
            return (self.client.invoke(invocation, request_id=self.request_id))

    def __guessMethod(self, methods, args):
        for method in methods:
            paramTypes = java.analyseParamTypes(self.__getParamType(method))
            if len(paramTypes) != len(args):
                continue
            ok = True
            for i in range(len(paramTypes)):
                pType = type(args[i])
                jType = paramTypes[i]
                if (pType == types.BooleanType and jType == 'bool') \
                        or (pType == types.DictType and jType == 'dict') \
                        or (pType == types.FloatType and jType == 'float') \
                        or (pType == types.IntType and jType == 'int') \
                        or (pType == types.IntType and jType == 'long') \
                        or (pType == types.ListType and jType == 'list') \
                        or (pType == types.LongType and jType == 'long') \
                        or (pType == types.StringType and jType == 'string') \
                        or (pType == types.TupleType and jType == 'list') \
                        or (pType == types.UnicodeType and jType == 'string') \
                        or (pType == _model.Object and jType == args[i]._metaType) \
                        or (pType == _model.Binary and jType == 'byte'):
                    continue
                elif pType == types.NoneType:
                    continue
                else:
                    ok = False
                    break
            if ok:
                return method
        return None

    def __getParamType(self, method):
        paramType = self.classInfo.constantPool[method['descriptorIndex']][2]
        paramType = paramType[paramType.find(b'(') + 1: paramType.find(b')')]
        return paramType

    def __getattr__(self, name):
        def dubbo_invoke(*args):
            return self.invoke(name, args)

        return dubbo_invoke


def _getAndDelConfigParam(config, key, default=None):
    if config and key in config:
        value = config[key]
        del config[key]
        return value
    else:
        return default


class Dubbo(object):
    def __init__(self, addrs, config=None, enable_heartbeat=False):
        if config:
            config = config.copy()
        else:
            config = {}
        self.config = config

        classpath = _getAndDelConfigParam(config, KEY_CLASSPATH)
        self.javaClassLoader = java.JavaClassLoader(classpath)

        owner = _getAndDelConfigParam(config, KEY_DUBBO_OWNER, DEFAULT_DUBBO_OWNER)
        customer = _getAndDelConfigParam(config, KEY_DUBBO_CUSTOMER, DEFAULT_DUBBO_CUSTOMER)
        self.attachments = {KEY_OWNER: owner, KEY_CUSTOMER: customer}

        for key, value in self.config.items():
            if key == KEY_REFERENCE:
                continue
            self.attachments[key] = value

        if KEY_REFERENCE not in self.config:
            self.config[KEY_REFERENCE] = {}

        self.client = DubboClient(addrs, self.config)

    def getObject(self, name):
        if type(name) == bytes:
            name = name.decode()
        return self.javaClassLoader.createObject(name)

    def getProxy(self, interface, **args):
        if type(interface) == str:
            interface = interface.encode()
        classInfo = self.javaClassLoader.findClassInfo(interface)
        if classInfo == None:
            return None
        attachments = self.attachments.copy()
        attachments[KEY_PATH] = interface
        attachments[KEY_INTERFACE] = interface

        if interface in self.config[KEY_REFERENCE]:
            attachments.update(self.config[KEY_REFERENCE][interface])

        if args:
            for key, value in args.items():
                attachments[key] = value

        self.__checkAttachments(attachments)

        return ServiceProxy(self.client, classInfo, attachments)

    def createConstObjectFromClass(self, className):
        return self.javaClassLoader.createConstObject(className)

    def __checkAttachments(self, attachments):
        if KEY_TIMEOUT not in attachments:
            attachments[KEY_TIMEOUT] = DEFAULT_TIMEOUT
        if KEY_VERSION not in attachments:
            attachments[KEY_VERSION] = DEFAULT_SERVICE_VERSION

    def close(self):
        print('Executors done!')
