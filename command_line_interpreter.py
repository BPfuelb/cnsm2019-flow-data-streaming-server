'''
command line interpreter for:
  a) changing the log level 
  b) specifying a dataset file for the file streaming server  
'''
import threading
from log import log, change_loglevel, logging
from constants import FILE_NAME


class Command_Line_Interpreter(threading.Thread):

  def __init__(self, send_file):
    threading.Thread.__init__(self)
    self.send_file = send_file
    
  def run(self):
    while(True):
      command = ''
      try:
        command = input('')
      except: 
        pass
      
      if command == 'd':
        change_loglevel(logging.DEBUG)
      if command == 'i':
        change_loglevel(logging.INFO)
      if command.startswith('FILE:'):
        FILE_NAME = command.split(':')[1]
        log.info('set filename: {}'.format(FILE_NAME))
        self.send_file()
