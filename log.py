'''
logging
'''
import logging
from constants import LOG_LEVEL

log_level = logging.DEBUG
if LOG_LEVEL == 'INFO':
  log_level = logging.INFO
elif LOG_LEVEL == 'ERROR':
  log_level = logging.ERROR
elif LOG_LEVEL == 'CRITICAL':
  log_level = logging.CRITICAL
elif LOG_LEVEL == 'WARNING':
  log_level = logging.WARNING
elif LOG_LEVEL == 'NOTSET':
  log_level = logging.NOTSET

# create logger
log = logging.getLogger('collector')
log.setLevel(log_level)
# create file handler which logs even debug messages
fh = logging.FileHandler('collector.log')
fh.setLevel(log_level)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(log_level)
# create formatter and add it to the handlers
# formatter = logging.Formatter('%(asctime)s, %(levelname)-5s %(message)s [%(filename)s:%(module)s:%(funcName)s:%(lineno)d]')
formatter = logging.Formatter('%(asctime)s, %(levelname)-5s %(message)s [%(filename)s:%(lineno)d]')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)


def change_loglevel(log_level):
  ''' change the current log level

  @param log_level: log level (int) (see logging module)
  '''  
  log.debug('change log level to: {}'.format(log_level))
  log.setLevel(log_level)
  fh.setLevel(log_level)
  ch.setLevel(log_level)
