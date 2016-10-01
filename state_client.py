import argparse
import state
import logging
import sys


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str, help='Server url')
    args = parser.parse_args()
    
    client = state.State.Client(args.url)
    for i in range(2):
        logging.info('Try send')
        id = client.add('1234')
        result = client.get(id)
        logging.info('Resp %s', result)
        id = client.update(id, '5678')
        result = client.get(id)
        logging.info('Updated %s', result)
        client.delete(id)
        logging.info('Delete %s', id)
   
