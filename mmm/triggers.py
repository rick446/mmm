import bson
import gevent
from collections import defaultdict

class Triggers(object):

    def __init__(self, connection, checkpoint):
        self._conn = connection
        self._oplog = self._conn.local.oplog.rs
        self._config = self._conn.local.mmm
        self._oplog.ensure_index('ts')
        self._callbacks = defaultdict(list)
        self._checkpoint = checkpoint

    def run(self):
        while True:
            spec = dict(ts={'$gt': self._checkpoint})
            q = self._oplog.find(
                spec, tailable=True, await_data=True)
            found=False
            print 'query on', self._oplog
            for op in q.sort('$natural'):
                found = True
                self._checkpoint = op['ts']
                for callback in self._callbacks.get(
                    (op['ns'], op['op']), []):
                    callback(**op)
            print found
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

    def store_checkpoint(self):
        self._config.update({}, {'$set': {'checkpoint': self._checkpoint } } )
