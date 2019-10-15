'''
file streaming server to replay stored datasets
'''
import socket
from connection_handler import Connection_Handler as CH
from connection_handler import client_threads
from log import log
from command_line_interpreter import Command_Line_Interpreter as CLI
from processor import Flow_Processor
from constants import *
import numpy as np
from datetime import datetime
import time
import gzip
import pickle
import os


def load_pickle_file(filename):
  ''' load a pickle file
  
  @param filename: local pickle filename/path to the pickle file (str)
  @return unpickled data
  '''  
  if not filename.endswith('.pkl.gz'): filename += '.pkl.gz'
  if os.path.dirname(os.path.realpath(__file__)) not in filename:
    folder_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
  else:
    folder_filename = filename
  if not os.path.isfile(folder_filename): assert('pickle file {} not found'.format(filename))
  
  with gzip.open(folder_filename, 'rb') as file:
    while True:
      try:
        yield pickle.load(file)
      except:
        raise StopIteration('no more data!')

    
def set_exit():
  global stop
  stop = True


def set_todo():
  global todo
  todo = send_file 


def send_file():
  ''' transfer a given file block by block
  
  1. load a preprocessed or raw dataset file
  2. process a (raw) data block if necessary
  3. transmit it while preserving a corresponding time interval (between data blocks)
  '''
  block = 0
  log.info('start file streamer loop ({})'.format(FILE_NAME))
  
  block_iterator = load_pickle_file(FILE_NAME)

  def get_end(block):
    ''' return the end timestamp of the block
    
    a) raw data (no aggregation etc.): end timestamp is after the collection of one data block with approximately 100.000 flows 
       (property keys: end, start)
    b) preprocessed data: end timestamp is after processing and enqueuing the data 
       (property keys: end, start)
    
    @param block: loaded block (dict(dict()))
    @return: end timestamp (datetime object)
    '''
    block_properties = block.get('properties')
    print(block_properties)
    return datetime.fromtimestamp(block_properties.get('end') or block_properties.get('end_collect'))
    
  initial_block = next(block_iterator)
  end = get_end(initial_block)
  
  print('end', end)
  
  current = initial_block
  if len(client_threads) == 0: 
    print('wait for one client')

  for next_ in block_iterator:
    while len(client_threads) == 0: 
      time.sleep(1)

    block += 1
    next_end = get_end(next_)  # load next data block to determine time to wait
    time_to_wait = (next_end - end).seconds
    
    if current.get('properties').get('end'):  # raw dataset
      log.info('preprocess data')
      start_process = time.time()
      Flow_Processor(current.get('data'), time.time(), export=False).start()  # auto enqueue
      end_process = time.time()
      process_delay = end_process - start_process
      print('process_delay', process_delay)
    else:  # preprocessed dataset
      log.info('enqueue data')
      CH.enqueue(np.array(current.get('data')))
      CH.wakeup()
      process_delay = 0
    
    if FIX_WAIT != -1: 
      time_to_wait = FIX_WAIT
    else: 
      time_to_wait -= process_delay
      if time_to_wait < 0: 
        time_to_wait = 0
           
    log.info('time to wait: {}s'.format(time_to_wait)) 
    time.sleep(time_to_wait)  # TODO: minus processing time (Flow_Processor)
        
    end = next_end
    current = next_
  
  CH.enqueue(np.array(current.get('data')))
  CH.wakeup()
    
  log.info('FILE END ({})'.format(FILE_NAME))

  
todo = send_file
stop = False
  
log.info('start command line interpreter')
command_line_interpreter = CLI(set_todo)
command_line_interpreter.start()

log.info('start connection handler')
connection_handler = CH()
connection_handler.start()

log.debug('bind socket for {}:{} with block size = {}'.format(HOST, COLLECTOR_PORT, BLOCK_SIZE))
socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
socket.bind((HOST, COLLECTOR_PORT))

log.info('start file streaming loop')
while not stop:
  time.sleep(1)
  if todo: todo()
  todo = None
