"""Simple state component."""

import logging
import sys
import threading
import uuid
import zmq


class State:
    """A State sever that stores data in memory.

    The server starts a background thread that serves a ZMQ REP url. 
    """
    logger = logging.getLogger('State')
    
    def __init__(self, url):
        """Initialize a state server at a given URL."""
        self.url = url
        context = zmq.Context.instance()
        self._socket = context.socket(zmq.REP)  # Reply
        self._socket.bind(self.url)
        self._stop = threading.Event()
        self.states = {} # (id -> object)

    def __del__(self):
        """Close the zmq socket when the object is deleted."""
        self._socket.close()
        
    def _run(self):
        """Main event loop for the server."""
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while not self._stop.is_set():
            socks = dict(poller.poll(10)) # milli-seconds
            if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                msg = self._socket.recv_json()
                self.logger.info('Received request %s', str(msg))
                result = self._process(msg)
                self._socket.send_json(result)
                
    def add(self, msg):
        """Add a data to the store."""
        data = msg.get('data')
        if data:
            id = str(uuid.uuid1())
            self.states[id] = data
            return {'status': 'OK', 'id': id}
        else:
            self.logger.error('No data to add')
            return {'status': 'ERROR'}
        
    def get(self, msg):
        """Get the data for the given id."""
        id = msg.get('id')
        if id:
            data = self.states.get(id)
            return {'status': 'OK', 'data': data}
        else:
            self.logger.error('No id provided')
            return {'status': 'ERROR'}

    def update(self, msg):
        """Update the data gor the given id."""
        id = msg.get('id')
        data = msg.get('data')
        if id and data:
            self.states[id] = data
            return {'status': 'OK', 'id': id}
        else:
            self.logger.error('Either id or data is missing')
            return {'status': 'ERROR'}

    def delete(self, msg):
        """Delete a record."""
        id = msg.get('id')
        if id:
            del self.states[id]
            return {'status': 'OK'}
        else:
            self.logger('No id is given to delete')
            return {'status': 'ERROR'}
            
    def _process(self, msg):
        """Switch yard."""
        if 'action' in msg:
            if msg.get('action') == 'add':
                return self.add(msg)
            elif msg.get('action') == 'get':
                return self.get(msg)
            elif msg.get('action') == 'update':
                return self.update(msg)
            elif msg.get('action') == 'delete':
                return self.delete(msg)
            else:
                self.logger.error('Unknown action')
                return {'status': 'ERROR'}
        else:
            self.logger.error('No action to perform')
                
    
    def start(self):
        """Start the server."""
        self._stop.clear()
        self._thread = threading.Thread(target=lambda : self._run())
        self._thread.start()

    def stop(self):
        """Stop the server."""
        self._stop.set()
        self._thread.join()

    def list(self):
        """List the content stored in the server."""
        print('States:')
        print(self.states)

    def get_client(self, url=None):
        """Get a client for the server."""
        if url:
            return State.Client(url)
        else:
            return State.Client(self.url)

    class Client:
        """The client to the state server."""
        
        logger = logging.getLogger('State.Client')
        
        def __init__(self, server_url):
            context = zmq.Context.instance()
            self._socket = context.socket(zmq.REQ)
            self._socket.connect(server_url)
            self._lock = threading.Lock()

        def add(self, data):
            """Add a data to the state.
            
            Args
              data - any json serializable data.
            Returns
              a string for the id of the data or None if no data.
            """
            with self._lock:
                self._socket.send_json({
                    'action': 'add',
                    'data': data })
                result = self._socket.recv_json()
            if result.get('status') == 'OK':
                return result.get('id')
            else:
                self.logger.error('Error while adding %s', data)
                return None

        def get(self, id):
            """Get the data from the state.
            
            Args
              id - string.
            Returns
              the data for the id.
            """
            with self._lock:
                self._socket.send_json({
                    'action': 'get',
                    'id': id})
                result = self._socket.recv_json()
            if result.get('status') == 'OK':
                return result.get('data')
            else:
                self.logger.error('Error while getting %s', id)
                return None

        def update(self, id, data):
            """Update the data for the given id.

            Args
              id: string
              data: any json serializable data.
            Returns
              a string for the id or None if error.
            """
            with self._lock:
                self._socket.send_json({
                    'action': 'update',
                    'id': id,
                    'data': data})
                result = self._socket.recv_json()
            if result.get('status') == 'OK':
                return result.get('id')
            else:
                self.logger.error('Error while updating %s', id)
                return None

        def delete(self, id):
            """Delete the data for the given id.

            Args
              id: string
            Returns
              Boolean, true if succeeded
            """
            with self._lock:
                self._socket.send_json({
                    'action': 'delete',
                    'id': id})
                result = self._socket.recv_json()
            if result.get('status') == 'OK':
                return True
            else:
                self.logger.error('Error while updating %s', id)
                return False
            
        
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    state_server = State('inproc://state')
    state_server.start()

    client = state_server.get_client()
    for i in range(1):
        logging.info('Try send')
        id = client.add('1234')
        result = client.get(id)
        logging.info('Resp %s', result)
        id = client.update(id, '5678')
        result = client.get(id)
        logging.info('Updated %s', result)
        client.delete(id)
        logging.info('Delete %s', id)
    state_server.stop()
    state_server.list()
