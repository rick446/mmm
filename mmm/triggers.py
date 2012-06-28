import logging
from collections import defaultdict

import gevent

log = logging.getLogger(__name__)

class Triggers(object):

    def __init__(self, connection, checkpoint):
        self._conn = connection
        self._oplog = self._conn.local.oplog.rs
        self._oplog.ensure_index('ts')
        self._callbacks = defaultdict(list)
        self.checkpoint = checkpoint

    def run(self):
        while True:
            yield self.checkpoint
            spec = dict(ts={'$gt': self.checkpoint})
            q = self._oplog.find(
                spec, tailable=True, await_data=True)
            found=False
            # log.debug('Query on %s', self._oplog)
            for op in q.sort('$natural'):
                found = True
                self.checkpoint = op['ts']
                for callback in self._callbacks.get(
                    (op['ns'], op['op']), []):
                    callback(**op)
            if found:
                gevent.sleep(0)
            else:
                gevent.sleep(1)

    def register(self, namespace, operations, func=None):
        def wrapper(func):
            for op in operations:
                self._callbacks[namespace, op].append(func)
            return func
        if func:
            return wrapper(func)
        else:
            return wrapper

