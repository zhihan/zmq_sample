"""Simple publisher that a user can subscribe to."""

import json
import logging
import sys
import time
import threading
import zmq


DELIMETER = '|'


class Publisher:
    """A publisher that publish data changes."""
    
    def __init__(self, url):
        """Initialize a publisher serving at a given URL.

        inproc sockets are not supported somehow.
        """
        self.url = url
        context = zmq.Context()
        self._socket = context.socket(zmq.PUB)
        self._socket.bind(url)

    def publish(self, data, prefix=''):
        """Publish a message on the socket.

        Args
          data: a dict that is serializable to json
        """
        # Use '|' as a delimiter between data and topic
        self._socket.send_string(prefix + DELIMETER + json.dumps(data))
        
    def __del__(self):
        self._socket.close()

    def get_subscriber(self, url=None, prefix=''):
        """Get a subscriber.
        
        Args
          url: a string of the socket url.
          prefix: a string used to filter subscription.
        """
        if url:
            return Publisher.Subscriber(url, prefix)
        else:
            return Publisher.Subscriber(self.url, prefix)

    class Subscriber:
        """A subscriber to the publisher."""
        logger = logging.getLogger('Publisher.Subscriber')
        
        def __init__(self, url, prefix):
            """Create a subscriber.
            
            Args
              url: string, the zmq socket url.
              prefix: subscription prefix, used for filtering. 
            """
            # ZMQ filter subscriptions by prefix.
            # See the reference of zmq_setsockopt.
            self.url = url
            self._prefix = prefix
            context = zmq.Context()
            self._socket = context.socket(zmq.SUB)
            self._socket.connect(url)
            self._socket.setsockopt_string(zmq.SUBSCRIBE, prefix)
            self._stop = threading.Event()

        def __del__(self):
            self._socket.close()

        def _run(self, on_update):
            poller = zmq.Poller()
            poller.register(self._socket, zmq.POLLIN)
            while not self._stop.is_set():
                socks = dict(poller.poll(10))
                if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                    s = self._socket.recv_string()
                    if self._prefix:
                        msg = json.loads(s[(len(self._prefix) + 1):])
                    else:
                        _, body = s.split(DELIMETER, 1)
                        msg = json.loads(body)
                    self.logger.debug('Received update %s', msg)
                    on_update(msg)
                    
        def start(self, on_update):
            """Start the subscription."""
            self._thread = threading.Thread(target=lambda : self._run(on_update))
            self._thread.start()

        def stop(self):
            """Stop the subscription."""
            self._stop.set()
            self._thread.join()        
        

if __name__ == '__main__':
    # A simple demonstration of the code
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

    url = 'ipc://pub'
    publisher = Publisher(url)

    # subscribed to only "1" messages
    subscriber = publisher.get_subscriber(prefix='1')
    subscriber.start(lambda msg: print('Got: %s' % msg))
    # subscribed to all messages
    sub2 = publisher.get_subscriber()
    sub2.start(lambda msg: print('Also got: %s' % msg))

    time.sleep(0.1)
    publisher.publish({'data': 'Hello'}, "1")
    publisher.publish({'data': 'world'}, "2")
    time.sleep(0.1)
    subscriber.stop()
    sub2.stop()
