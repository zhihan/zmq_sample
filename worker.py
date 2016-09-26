"""Sinple worker component."""

import json
import logging
import sys
import time
import threading

import publisher
import zmq

class Worker:
    """A simple worker pattern. 

    A worker receives work from request, and publish progress using publisher."""
    def __init__(self, id, url, publisher_url):
        self.id = id
        self.url = url
        context = zmq.Context.instance()
        self._socket = context.socket(zmq.REP)
        self._socket.bind(self.url)
        self._stop = threading.Event()
        
        self.publisher = publisher.Publisher(publisher_url)
        
    def _run(self):
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while not self._stop.is_set():
            socks = dict(poller.poll(10))
            if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                msg = self._socket.recv_json()
                self._socket.send_json(
                    {'status': 'OK',
                     'worker-id': self.id})
                self._work(msg)

    def _work(self, msg):
        print('Working on %s' %(msg))
        time.sleep(0.1)
        print('Finished')
        self.publisher.publish({'status': 'done'}, prefix=self.id)
    

    def __del__(self):
        self._socket.close()
        
    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=lambda : self._run())
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()

    def get_client(self, url=None):
        if not url:
            return Worker.Client(url)
        else:
            return Worker.Client(self.url)
        

if __name__ == '__main__':
    worker = Worker('1', 'inproc://worker', 'ipc://worker-1')
    worker.start()
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect('inproc://worker')

    subscriber = worker.publisher.get_subscriber(prefix='1')
    subscriber.start(on_update=lambda msg: print('Update (1) %s' % msg))
    time.sleep(0.5)

    socket.send_json({'work': 'something'})
    result = socket.recv_json()
    print('Result %s' % result)

    time.sleep(0.5)
    subscriber.stop()
    worker.stop()
