# A test client for standalone worker server.
import argparse
import logging
import sys
import time

import zmq

import publisher
import worker

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker_id', type=str, help='Worker id')
    parser.add_argument('--publish_url', type=str, help='Publish url')
    parser.add_argument('--worker_url', type=str, help='Worker service url')
    args = parser.parse_args()

    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.connect(args.worker_url)

    subscriber = publisher.Publisher.Subscriber(
        url=args.publish_url, prefix=args.worker_id)
    subscriber.start(on_update=lambda msg: print('Update %s' % msg))
    time.sleep(0.5)

    for i in range(5):
        socket.send_json({'id': 'aaa', 'data': 'Hello world!'})
        result = socket.recv_json()
        print('Result %s' % result)

    time.sleep(0.5)
    subscriber.stop()
