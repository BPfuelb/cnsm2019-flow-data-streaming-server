'''
handler for client connections
'''

import threading
import socket
import pickle        
import zlib       
from connection import Connection
from log import log
from constants import *

client_threads = []


class Connection_Handler(threading.Thread):

  def __init__(self):
    ''' class (thread) that manages client connections '''
    threading.Thread.__init__(self)
    self.socket = socket.socket()          
    
    self.socket.bind((CLIENT_HOST, CLIENT_PORT)) 
    self.socket.listen()
    
  @classmethod 
  def enqueue(cls, data):
    ''' enqueue data  
    
    @param data: data to put in each clients individual data queue
    '''      
    data = pickle.dumps(data)
    data = zlib.compress(data)
    log.debug('enqueue data ({} bytes)'.format(len(data)))
    for client_thread in client_threads: 
      client_thread._enqueue(data)
      
  @classmethod 
  def wakeup(cls):
    ''' wake up all client threads, remove dead client connections/threads  '''
    log.debug('wake up all client threads ({})'.format(len(client_threads)))
    for client_thread in client_threads:
      if not client_thread.is_alive():
        log.warning('remove thread from thread ({}) list'.format(client_thread))
        client_threads.remove(client_thread)
        continue
      try:
        client_thread.condition.acquire()
        client_thread.condition.notify()
      finally:
        client_thread.condition.release()
    
  def run(self):
    ''' loop for handling incoming client connections '''
    log.info('start Connection Handler for {}:{}'.format(CLIENT_HOST, CLIENT_PORT))
    while True:
      connection, client_address = self.socket.accept()
      log.info('got new connection from {}:{}'.format(*client_address))
      client_thread = Connection(connection, client_address)
      client_thread.start()
      client_threads.append(client_thread)
      
    self.socket.close()
