from dubbo import Dubbo
from dubbo._model import Object
import asyncio


config = { 'classpath' : '' }
client = Dubbo((('', 0),), config)
q = client.getProxy('')

async def fuck():
    print(await(q.queryStoreAllInfoById(41)))
loop = asyncio.get_event_loop()
asyncio.ensure_future(fuck())
loop.run_forever()


