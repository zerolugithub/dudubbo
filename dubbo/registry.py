"""
Zookeeper registry
"""
import kazoo.client
from urllib.parse import unquote

class ZookeeperRegistry:
    _DEFAULT_ZOOKEEPER_PORT = 2181
    _DEFAULT_CONNECTION_TIMEOUT = 3

    def __init__(self, server, timeout=_DEFAULT_CONNECTION_TIMEOUT):
        self._zk = None
        self._server = server
        self._timeout = timeout
        self.connect()

    def connect(self):
        if self._zk and self._zk.connected:
            # Already connected and not changing server.
            return
        print('\033[92m\033[1m[Zookeeper]\033[0m Connecting to {} (timeout: {}s)...'
              .format(self._server, self._timeout))
        self._zk = kazoo.client.KazooClient(hosts=self._server,
                                            timeout=self._timeout)
        self._zk.start()
        self._zk.add_listener(self.event_handler)

    def get_providers(self, interface):
        self.connect()
        path = '/dubbo/%s/providers' % interface
        return [unquote(i)[8:].split('/', 1)[0] for i in self._zk.get_children(path) if i.startswith('dubbo')]

    def event_handler(self, event):
        print('zookeeper event:', event)
        # Not implement

if __name__ == '__main__':
    registry = ZookeeperRegistry('127.0.0.1:2181')
    print(registry.get_providers('com.A.B.C.D'))