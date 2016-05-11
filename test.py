from dubbo import Dubbo
from dubbo._model import Object


config = { 'classpath' : 'oop-api-client-1.0.7.jar' }
client = Dubbo((('xxx', 18005),), config)
#client = Dubbo((('127.0.0.1', 38100),), config)
#a = client.getObject('com.dmall.oop.dubbo.vo.VenderCondition')
#a.venderId = 41
q = client.getProxy('com.yyy.jjj.dubbo.JJJ.db.XXX')
asyncio.get_event_loop().run_until_complete(q.DJB(41)).model


