'''
client connection
'''
import threading
import queue
from log import log
from utils import measure_time_memory
from constants import *
import struct


class Connection(threading.Thread):

  def __init__(self, connection, client_address):
    ''' class (thread) that handles/represents a client connection 
    @param connection: socket for sending/receiving data on the client connection
    @param client_address: client socket address
    '''      
    
    threading.Thread.__init__(self)
    
    self.socket = connection
    self.client_address = client_address
    self.setName(client_address)
    self.condition = threading.Condition(threading.Lock())
    self.data_queue = queue.Queue()  # client-specific data queue 
    
    self.num_connection_errors = 0
  
  def _enqueue(self, data):
    ''' enqueue data  
    
    @param data: data to put in the queue
    '''      
    self.data_queue.put(data)
  
  @measure_time_memory  
  def send_data(self):
    ''' dequeue data, determine its length and send both to the client '''      
    data = self.data_queue.get()
    log.debug('send_data_bytes: {} bytes to {}'.format(len(data), self.client_address))
    try:
      num_bytes = len(data)
      num_bytes = struct.pack('!I', num_bytes)
      self.socket.send(num_bytes + data)
    except Exception as ex:
      log.warning('Exception: {}'.format(ex))
      self.num_connection_errors += 1
      del(ex)
  
  def run(self):
    ''' loop for sending available preprocessed data to the connected client '''
    while True:
      if not self.data_queue.empty():  # data available -> send it
        self.send_data()
        continue
      
      if self.num_connection_errors >= MAX_CONNECTION_ERRORS:  # maximum connection errors exceeded -> terminate
        log.error('connection retries exceeded: exit ({}:{})'.format(*self.client_address))
        break
        
      log.debug('no data in queue...goto sleep ({})'.format(self.client_address))
      try:  # wait until data is available
        self.condition.acquire()
        self.condition.wait()
      finally:
        self.condition.release()
    
    try:
      self.socket.close()
    except Exception as ex: 
      log.warning('could not close connection: {}'.format(ex))
    del(self.socket)
