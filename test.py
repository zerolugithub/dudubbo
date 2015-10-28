from dubbo import Dubbo
from dubbo._model import Object


config = { 'classpath' : 'oop-api-client-1.0.7.jar' }
client = Dubbo((('119.254.97.159', 18005),), config)
#client = Dubbo((('127.0.0.1', 38100),), config)
#a = client.getObject('com.dmall.oop.dubbo.vo.VenderCondition')
#a.venderId = 41
q = client.getProxy('com.dmall.oop.dubbo.interfaces.db.StoreInfoServiceForDB')
print(q.queryStoreAllInfoById(41).model)


