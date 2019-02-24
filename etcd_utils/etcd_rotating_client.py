from etcd_utils import EtcdEndpoint
from typing import List
import etcd3  # type: ignore
import logging

log = logging.getLogger(__name__)


class EtcdConnectionError(Exception):
    pass


class EtcdRotatingClient:
    def __init__(self,
                 endpoints,
                 retry_reconnect=3
                 ):
        self.endpoints: List[EtcdEndpoint] = endpoints
        self.current_index = None
        self.retry_reconnect = 3
        self._client = None

    def rotate(self):
        self._client = None
        if not self.current_index:
            self.current_index = 0
        self.current_index += 1
        if self.current_index >= len(self.endpoints):
            self.current_index = 0
        return self.current_endpoint()

    def current_endpoint(self):
        if self.current_index is None:
            return None
        return self.endpoints[self.current_index]

    @property
    def client(self):
        if self._client is None:
            i = 0
            while i < self.retry_reconnect:
                try:
                    self.rotate()
                    ep = self.current_endpoint()
                    log.info('etcd client using endpoint %s' % ep)
                    self._client = etcd3.Client(
                        host=ep.address,
                        port=ep.port,
                        timeout=ep.timeout)
                    self._client.status()
                    break
                except Exception:
                    log.exception('error connecting to etcd')
                i += 1
            if i == self.retry_reconnect:
                raise EtcdConnectionError('failed connecting to etcd '
                                          'after %s retries' % i)
        return self._client
