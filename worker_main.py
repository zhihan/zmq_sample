import argparse
import logging
import sys

import worker
import publisher


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker_id', type=str, help='Worker id')
    parser.add_argument('--publish_url', type=str, help='Publish url')
    parser.add_argument('--worker_url', type=str, help='Worker service url')
    args = parser.parse_args()

    logging.info('Serving worker at %s', args.worker_url)
    my_worker = worker.Worker(args.worker_id, args.worker_url, args.publish_url)
    my_worker.start()


