import time

import bson
import pymongo
import gevent

from triggers import Triggers
from replication import Replicator

def main():
    conn1 = pymongo.Connection(port=27019)
    conn2 = pymongo.Connection(port=27017)
    triggers = {
        conn1: init_triggers(conn1),
        conn2: init_triggers(conn2) }
    foo, bar = conn1.test.foo, conn2.test.bar
    start_replicating(triggers, foo, bar)
    start_replicating(triggers, bar, foo)
    gevent.spawn_link_exception(checkpoint, triggers, 1)
    greenlets = [
        gevent.spawn_link_exception(t.run)
        for t in triggers.values() ]
    gevent.joinall(greenlets)

def init_triggers(conn):
    config = conn.local.mmm.find_one()
    if config is None:
        config = dict(
            checkpoint=bson.Timestamp(long(time.time()), 1))
        conn.local.mmm.save(config)
    return Triggers(conn, config['checkpoint'])

def checkpoint(triggers, period):
    while True:
        gevent.sleep(period)
        for t in triggers.values():
            t.store_checkpoint()


def start_replicating(triggers, source, dest):
    r = Replicator(bson.ObjectId(), dest)
    dest_conn = dest.database.connection
    dest_db_name = dest.database.name
    dest_coll_name = dest.coll.name
    triggers[dest_conn].register(
        '%s.%s' % (dest_db_name, dest_coll_name), 'iud', r)

if __name__ == '__main__':
    main()

