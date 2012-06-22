import bson

class Replicator(object):

    def __init__(self, id, collection):
        self.id = id
        self._collection = collection

    def __call__(self, ts, h, op, ns, o, o2=None, b=False):
        print ts, op, ns
        if op == 'i':
            if o.get('_repl') == self.id:
                print 'SKIP'
                return
            o['_repl'] = self.id
            self._collection.insert(o)
        elif op == 'u':
            upsert = b
            setters = o.setdefault('$set', {})
            if setters.get('_repl') == self.id:
                print 'SKIP'
                return
            setters.setdefault('_repl', self.id)
            self._collection.update(o2, o, upsert)
        elif op == 'd':
            justOne = b
            self._collection.remove(o)
