"""Simple worker component."""

import logging
import sys
import time
import threading

import publisher
import zmq


class Worker:
    """A simple worker pattern. 

    A worker receives work from request, and publish progress using publisher.
    """
    logger = logging.getLogger('Worker')
    
    def __init__(self, worker_id, url, publisher_url):
        """Create a worker service."""
        self.worker_id = worker_id
        self.url = url
        context = zmq.Context.instance()
        self._socket = context.socket(zmq.REP)
        self._socket.bind(self.url)
        self._stop = threading.Event()
        self._thread = None
        
        self.publisher = publisher.Publisher(publisher_url)
        
    def _run(self):
        """Run the worker thread using the zmq poller."""
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while not self._stop.is_set():
            socks = dict(poller.poll(10))
            if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                msg = self._socket.recv_json()
                self._socket.send_json(
                    {'status': 'OK',
                     'id': msg.get('id'),
                     'data': msg.get('data'),
                     'worker-id': self.worker_id})
                self._work(msg)

    def _work(self, msg):
        self.logger.info('Working on %s', msg)
        time.sleep(0.5)
        self.logger.info('Worker %s finished %s', self.worker_id, msg.get('id'))
        self.publisher.publish({'status': 'done',
                                'id': msg.get('id'),
                                'worker-id': self.worker_id}, 
                               prefix=self.worker_id)
    

    def __del__(self):
        self._socket.close()
        
    def start(self):
        """Start the worker service."""
        self._stop.clear()
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        """Stop the worker service."""
        self._stop.set()
        self._thread.join()
        

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    worker = Worker('1', 'inproc://worker', 'ipc://worker-1')
    worker.start()
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.connect('inproc://worker')

    subscriber = worker.publisher.get_subscriber(prefix='1')
    subscriber.start(on_update=lambda msg: print('Update (1) %s' % msg))
    time.sleep(0.5)

    socket.send_json({'id': 'aaa', 'data': 'something'})
    result = socket.recv_json()
    print('Result %s' % result)

    time.sleep(0.5)
    subscriber.stop()
    worker.stop()
