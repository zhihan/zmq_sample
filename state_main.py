import argparse
import state
import logging
import sys


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str, help='Server url')
    args = parser.parse_args()
    
    state_server = state.State(args.url)
    state_server.start()
