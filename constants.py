'''
constants definition
'''
LOG_LEVEL = 'DEBUG'  # log level

HOST = '0.0.0.0'  # IP for the host interface
COLLECTOR_PORT = 8877  # port of the flow collector
BLOCK_SIZE = 100000  # number of flows that form a block
BUFFER_SIZE = 2 ** 11

CLIENT_HOST = HOST  # IP for the host interface
CLIENT_PORT = 11338  # port for client connections
SOCKET_TIMEOUT = 60 * 1  # 1 minute

MAX_CONNECTION_ERRORS = 3  # maximum number of connection errors until the socket is closed

#-------------------------------------------------------------------------------- only for file streamer
FIX_WAIT = -1  # if -1 use time differences between data blocks, else use fixed wait time for each block
# FILE_NAME = 'data/raw_data.pkl.gz'
FILE_NAME = 'data/data.pkl.gz'
