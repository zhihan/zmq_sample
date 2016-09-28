"""Simple dispatcher component."""

import json
import logging
import sys
import time
import zmq

import publisher
import state
import worker

class Dispatcher:
    logger = logging.getLogger('Dispatcher')
    """A simple dispatcher.
    
    A dispatcher connect to one or more workers and uses state server to store
    the data.
    """
    def __init__(self, worker_urls, worker_subscriptions, state_url):
        self.worker_urls = worker_urls
        self.worker_subscriptions = worker_subscriptions
        self.state_url = state_url
        context = zmq.Context.instance()

        self.worker_socket = context.socket(zmq.REQ)
        for worker_url in worker_urls:
            self.worker_socket.connect(worker_url)

        self.worker_subscribers = []
        for sub in worker_subscriptions:
            subscriber = publisher.Publisher.Subscriber(sub)
            subscriber.start(on_update=lambda msg: self.on_update(msg))
            self.worker_subscribers.append(subscriber)
        
        self.state_client = state.State.Client(state_url)

    def stop(self):
        """Stop all the subscriber threads."""
        for sub in self.worker_subscribers:
            sub.stop()        
        
    def do_work(self, data):
        self.logger.info('Adding work %s', data)
        work_id = self.state_client.add(data)
        self.worker_socket.send_json({
            "id": work_id,
            "data": data})
        self.logger.info('Sending work %s to workers', work_id)
        result = self.worker_socket.recv_json()
        self.logger.info('Work %s has started' % work_id)

    def on_update(self, msg):
        id = msg.get('id')
        self.logger.info('Update: work %s finished' % id)
        data = self.state_client.get(id)
        data = {'data': data,
                'status': 'Completed'}
        self.state_client.update(id, data)

        
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    state_url = 'inproc://state'
    state_server = state.State(state_url)
    state_server.start()

    worker1_url = 'inproc://worker1-server'
    worker1_pub = 'ipc://worker1'
    worker1 = worker.Worker('1', worker1_url, worker1_pub)
    worker1.start()

    worker2_url = 'inproc://worker2-server'
    worker2_pub = 'ipc://worker2'
    worker2 = worker.Worker('2', worker2_url, worker2_pub)
    worker2.start()

    
    dispatcher = Dispatcher([worker1_url, worker2_url],
                            [worker1_pub, worker2_pub],
                            state_url)

    for i in range(2):
        dispatcher.do_work('Hello')
        dispatcher.do_work('World')

    time.sleep(1)
    dispatcher.stop()
    
    state_server.list()
    state_server.stop()
    worker1.stop()
    worker2.stop()
    
