========
Usage
========

You can find examples in ``etcd3gw/examples`` and look at ``etcd3gw/client.py``.

Basic usage example::

    from etcd3gw.client import Etcd3Client
 
    client = Etcd3Client(host='localhost', port=2379)

    # Put key
    client.put(key='foo', value='bar')

    # Get key
    client.get(key='foo')

    # Get all keys
    client.get_all()


    # Create lease and use it
    lease = client.lease(ttl=100)

    client.put(key='foo', value='bar', lease=lease)

    # Get lease keys
    lease.keys()

    # Refresh lease
    lease.refresh()


    # Use watch
    watcher, watch_cancel = client.watch(key='KEY')

    for event in watcher: # blocks until event comes, cancel via watch_cancel()
        print(event)
        # modify event: {'kv': {'mod_revision': '8', 'version': '3', 'value': 'NEW_VAL', 'create_revision': '2', 'key': 'KEY', 'lease': '7587847878767953426'}}
