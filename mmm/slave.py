import time
import gevent

import bson
from pymongo import Connection

from .triggers import Triggers

MMM_DB_NAME = 'mmm'
MMM_REPL_FLAG = '__mmm'

class ReplicationSlave(object):
    '''Sets up replication based on info in the local 'mmm' collection.

    Each 'master' connection has its own document in mmm:
    
    {  _id: 'mongodb://master_connection_uri',
      checkpoint: ...,  // timestamp offset in the oplog
      replication: [
         { dst: 'slave-database.collection',
           src: 'master-database.collection' },
         ... ]
    }
    '''

    def __init__(self, uri):
        self._conn = Connection(uri, use_greenlets=True)
        self._coll = self._conn.local[MMM_DB_NAME]
        self._config = {}
        self._greenlets = []

    def start(self, checkpoint=None):
        for gl in self._greenlets:
            gl.kill()
        self.load_config()
        self._greenlets = [
            gevent.spawn_link_exception(self.periodic_checkpoint, 5) ]
        for master_uri in self._config:
            self._greenlets.append(
                gevent.spawn_link_exception(
                    self.replicate, master_uri, checkpoint))

    def load_config(self):
        self._config = {}
        for master in self._coll.find():
            self._config[master['_id']] = master

    def set_replication(self, master_uri, ns_dst, ns_src):
        replication = dict(dst=ns_dst, src=ns_src)
        master = self._coll.update(
            dict(_id=master_uri),
            { '$addToSet': { 'replication': replication } },
            upsert=True,
            new=True)
        self._config[master_uri] = master

    def unset_replication(self, master_uri, ns_dst=None, ns_src=None):
        to_pull = dict()
        if ns_dst is not None: to_pull['dst'] = ns_dst
        if ns_src is not None: to_pull['src'] = ns_src
        if to_pull:
            # Stop replication on one namespace
            master = self._coll.find_and_modify(
                dict(_id=master_uri),
                { '$pull': { 'replication': to_pull } },
                new=True)
            self._config[master_uri] = master
        else:
            # Stop replication on the whole master
            self._coll.remove(dict(_id=master_uri))
            self._config.pop(master_uri, None)
        
    def checkpoint(self, master_uri=None):
        if master_uri is None:
            masters = self._config.items()
        else:
            masters = [
                (master_uri, self._config[master_uri]) ]
        for _id, master in masters:
            self._coll.update(
                dict(_id=_id),
                { '$set': { 'checkpoint': master['checkpoint'] } })

    def replicate(self, master_uri, checkpoint=None):
        '''Actual replication loop for replicating off of master_uri'''
        master = self._config[master_uri]
        conn = Connection(master_uri, use_greenlets=True)
        if checkpoint is None:
            checkpoint = master.get('checkpoint')
        if checkpoint is None:
            # By default, start replicating as of NOW
            checkpoint = bson.Timestamp(long(time.time()), 0)
        triggers = Triggers(conn, checkpoint)
        for r in master['replication']:
            triggers.register(
                r['src'], 'iud', self._replicate_to_trigger(r['dst']))
        for checkpoint in triggers.run():
            master['checkpoint'] = checkpoint

    def periodic_checkpoint(self, period=1.0):
        '''Periodically call self.checkpoint() to allow restarts'''
        while True:
            gevent.sleep(period)
            self.checkpoint()

    def _replicate_to_trigger(self, dst):
        repl_id = bson.ObjectId()
        db, cname = dst.split('.', 1)
        collection = self._conn[db][cname]
        def trigger(ts, h, op, ns, o, o2=None, b=False):
            print ts, op, ns
            if op == 'i':
                if o.get(MMM_REPL_FLAG) == repl_id:
                    print 'SKIP'
                    return
                o[MMM_REPL_FLAG] = repl_id
                collection.insert(o)
            elif op == 'u':
                upsert = b
                setters = o.setdefault('$set', {})
                if setters.get(MMM_REPL_FLAG) == repl_id:
                    print 'SKIP'
                    return
                setters.setdefault(MMM_REPL_FLAG, repl_id)
                collection.update(o2, o, upsert)
            elif op == 'd':
                justOne = b
                collection.remove(o)
        return trigger

    def __getitem__(self, ns_dst):
        return _ReplLHS(self, ns_dst)

class _ReplLHS(object):

    def __init__(self, slave, ns_dst):
        self.slave = slave
        self.ns_dst = ns_dst

    def __lshift__(self, (master_uri, ns_src)):
        self.slave.set_replication(
            master_uri, ns_dst=self.ns_dst, ns_src=ns_src)
        return self
        
