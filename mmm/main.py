import gevent

from .slave import ReplicationSlave

def main():
    a_uri = 'mongodb://localhost:27019'
    b_uri = 'mongodb://localhost:27017'
    s_a = ReplicationSlave(a_uri)
    s_b = ReplicationSlave(b_uri)
    # Make test.foo on a_uri be a slave of test.bar on b_uri
    s_a['test.foo'] << (b_uri, 'test.bar')
    # Make test.bar on b_uri be a slave of test.foo on a_uri
    s_b['test.bar'] << (a_uri, 'test.foo')
    s_a.start()
    s_b.start()
    while True:
        gevent.sleep(5)
        print '=== mark ==='

if __name__ == '__main__':
    main()

