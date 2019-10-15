'''
processing unit to prepare flow data for model training and testing (side: flow data streaming server)
'''
from threading import Thread
import numpy as np
from ipaddress import IPv4Address
from connection_handler import Connection_Handler as CH
from log import log
from datetime import datetime
from utils import measure_time_memory
import time
import sys
import multiprocessing
import pickle
import gzip
import os

np.set_printoptions(threshold=sys.maxsize)  # print whole numpy arrays

# filtering data that matches specified feature values (lambda expressions)
feature_filter = {
  # e.g., excluding DNS traffic
	# (6,): lambda x: x == 53,
	# (7,): lambda x: x == 53,
  # (6,7): lambda x,y: x == 53 or y == 53,
  }

# region ---------------------------------------------------------------------------------------------- AGGREGATION INIT
UDP_TIMEOUT = 30000  # ms; timeout for considering flow records with identical 5-tuple as separate communications


class Feature():
  ''' enumeration class defines positions of features '''
  # DATA
  SRC_ADDR = 0
  DST_ADDR = 1
  PACKETS = 2
  BYTES = 3
  FIRST_SWITCHED_TS = 4
  LAST_SWITCHED_TS = 5
  SRC_PORT = 6
  DST_PORT = 7
  TCP_FLAGS = 8
  PROTOCOL = 9
  EXPORT_HOST = 10
  FLOW_SEQ_NUM = 11
  DURATION = 12
  BIT_RATE = 13
  # LOOKUP SRC
  SRC_COUNTRY_CODE = 14
  SRC_LONGITUDE = 15
  SRC_LATITUDE = 16
  SRC_ASN = 17
  SRC_NETWORK = 18
  SRC_PREFIX_LEN = 19
  SRC_VLAN = 20
  SRC_LOCALITY = 21
  # LOOKUP DST
  DST_COUNTRY_CODE = 22
  DST_LONGITUDE = 23
  DST_LATITUDE = 24
  DST_ASN = 25
  DST_NETWORK = 26
  DST_PREFIX_LEN = 27
  DST_VLAN = 28
  DST_LOCALITY = 29
  # FIRST SWITCHED TIMESTAMP
  FS_YEAR = 30
  FS_MONTH = 31
  FS_DAY = 32
  FS_HOUR = 33
  FS_MINUTE = 34
  FS_SECOND = 35


FIVE_TUPLE = [  # definition of 5-tuple features
  Feature.PROTOCOL,
  Feature.SRC_ADDR,
  Feature.DST_ADDR,
  Feature.SRC_PORT,
  Feature.DST_PORT,
  ]


class TCPFlags():
  ''' defines general TCP flags (used for aggregation) '''
  NONE = 0
  FIN = 1
  SYN = 2
  RST = 4
  PSH = 8
  ACK = 16
  URG = 32
# endregion


# region ----------------------------------------------------------------------------------------------- ENRICHMENT INIT
import geo
log.info('load geo data')
geo.load_data()

import prefix_lookup
log.info('create prefix lookup data')
prefix_lookup.load_prefix_data()
# endregion

# region ---------------------------------------------------------------------------------------------- REPLACEMENT INIT
PORT_MAX_LIMIT = 2 ** 15  # -1 -> no maximum limit respectively no replacement
# endregion

# region ----------------------------------------------------------------------------------------------- ANONYMIZER INIT
PERMUTATION_SEED = b'foobar'
import hashlib

# hash PERMUTATION_SEED (SHA3-512)
sha3_512_hash = hashlib.sha512(PERMUTATION_SEED).hexdigest()

# create 4 seeds (from sha3_512_hash)
seeds = [
  int(sha3_512_hash[0:8], 16),
  int(sha3_512_hash[56:64], 16),
  int(sha3_512_hash[64:72], 16),
  int(sha3_512_hash[120:128], 16),
  ]

# create 4 substitution tables, one for each octet of an ip address
# seeds are used for the permutation of individual substitution tables 
global SUBSTITUTION_TABLES
SUBSTITUTION_TABLES = [ np.random.RandomState(seed=seed).permutation(np.arange(256)) for seed in seeds ]
# SUBSTITUTION_TABLES = [ np.arange(256) for seed in seeds ] # only for testing (disable the anonymization)
SUBSTITUTION_TABLES = np.array(SUBSTITUTION_TABLES, dtype=np.uint8)
# endregion

log.debug('Flow Processor parameter: PORT_MAX_LIMIT={}'.format(PORT_MAX_LIMIT))
log.debug('Flow Processor parameter: UDP_TIMEOUT={}'.format(UDP_TIMEOUT))

NUM_PROCESSES = multiprocessing.cpu_count() - 1
result_queue = multiprocessing.Queue()  # synchronized queue for results


def foo(args):
  ''' aggregate, enrich, format (incl. replacing random values) and anonymize data chunk-wise (each chunk is handled by one process of the process pool)
  
  @param args: list of arguments
    * [0]: id of the data chunk (int)
    * [1]: slices of indices with unique 5-tuples (np.array)
    * [2]: full data (np.array)
    * [3]: only 5-tuple data (np.array)
   '''
  id_ = args[0]
  unique_5tuples_indices = args[1]
  data = args[2]
  data_ = args[3]

  data = aggregate(data, data_, unique_5tuples_indices)
  data = enrich(data)
  data = format_(data)
  data = replace_randoms(data)
  data = anonymize(data)

  result_queue.put((id_, data))


@measure_time_memory
def filter_features(data, feature_filter):
  ''' feature filter based on defined lambda expression(s)

  @param data: input data (np.array)
  @param feature_filter: feature filter dict (dict)
  @return: filtered data (np.array)
  '''
  for feature_key, lambda_exp in feature_filter.items():
    mask = np.squeeze(np.vectorize(lambda_exp)(*data[:, feature_key].T))
    mask = np.invert(mask)
    data = data[mask]
  return data


# select indices of first occurrence of each unique 5-tuple
@measure_time_memory
def aggregate(data, data_, unique_5tuples_indices):
  ''' aggregate flow records 
  
  for each unique 5-tuple:
    1. select indices for data with an equal 5-tuple
    2. sort the corresponding flow records based on the first switched timestamp and flow sequence number
    3. combine all flow records with the same 5-tuple:
    (OR) 3a. if flow records belong to TCP see tcp()
    (OR) 3b. if flow records belong to UDP see udp()
    (OR) 3c. neither TCP nor UDP -> append to aggregated flows
  @param data: flow data (np.array)
  @param data_: only 5-tuples of flow data (np.array)
  @param unique_5tuples_indices: indices for unique 5-tuple information
  @return: aggregated flow records/flow entries (np.array)
  '''  
  aggregated_data = []
  for i in unique_5tuples_indices:

    # region ------------------------------------------------------------------ 1. select indices for data with an equal 5-tuple by a mask
    mask = data_[:, ] == data_[i]
    mask = np.all(mask, axis=1)
    equal_5tuples = data[mask]  # all flow records with a matching 5-tuple
    # endregion

    # region --------------------------------------------- 2. sort flow records based on first switched timestamp and flow sequence number
    # The last key in the sequence (FIRST_SWITCHED_TS) is used for the primary sort order,
    # the second-to-last key (FLOW_SEQ_NUM) for the secondary sort order, and so on.
    sort_indices_5tuples = np.lexsort((equal_5tuples[:, Feature.FLOW_SEQ_NUM], equal_5tuples[:, Feature.FIRST_SWITCHED_TS]))
    equal_5tuples = equal_5tuples[sort_indices_5tuples]
    # endregion

    def udp(data_udp):
      ''' aggregate udp flow records for a unique udp-5-tuple

        0. filter flow records based on export host (first appearance)
        1. calculate time distances between flow records
        2. determine flow records that exceed the specified UDP timeout (see UDP_TIMEOUT) -> stop_indices
        for each stop index:
          3. calculate time gaps between flow records -> time
          4. calculate sum of #packets/#bytes, duration (minus time) and bit rate
          5. append result to aggregated flows (flow entries)

      @param data_udp: flow records for a unique udp-5-tuple (np.array)
      '''
      # region -------------------------------------------- 0. filter flow records based on export host (first appearance)
      # select flow records based on export host from first record, other records are dropped
      data_udp = data_udp[data_udp[:, Feature.EXPORT_HOST] == data_udp[0, Feature.EXPORT_HOST]]
      # endregion

      # region ----------------------------------------------------------- 1. calculate time distances between flow records
      time_diff = data_udp[:, [Feature.FIRST_SWITCHED_TS, Feature.LAST_SWITCHED_TS]]  # get only first and last switched timestamp
      time_diff = time_diff.reshape(-1, 1)  # reshape to one column
      time_diff = np.squeeze(time_diff)  # remove single-dimensional entries
      time_diff = np.diff(time_diff)  # calculate differences: ls_n - fs_n, fs_n+1 - ls_n

      time_diff_ = time_diff[1::2]
      # endregion
      # region ----------------------------------------- 2. determine flow records that exceed the UDP timeout (see UDP_TIMEOUT)
      stop_indices = np.where(time_diff_ > UDP_TIMEOUT)[0] 

      start_index = 0
      stop_indices = stop_indices.tolist()
      stop_indices.append(len(data_udp) - 1)
      # endregion
      for stop_index in stop_indices:
        # region ------------------------------------------------------------------- 3. calculate time gaps between flow records
        stop_index += 1

        sum_packets = np.sum(data_udp[start_index: stop_index, Feature.PACKETS])
        sum_bytes = np.sum(data_udp[start_index: stop_index, Feature.BYTES])
        max_ls = np.max(data_udp[start_index: stop_index, Feature.LAST_SWITCHED_TS])

        time_diff_slice = time_diff[start_index * 2: stop_index * 2]
        i1 = np.where(time_diff_slice < UDP_TIMEOUT)[0]
        i2 = np.where(time_diff_slice > 0)[0]
        i3 = np.intersect1d(i1, i2)
        i3 = i3[i3 % 2 == 1]
        time = np.sum(time_diff_slice[i3])
        if not time: 
          time = 0.
        # endregion
        # region ------------------------------- 4. calculate sum of #packets/#bytes, duration (minus time) and bit rate
        data_udp[start_index, Feature.PACKETS] = sum_packets
        data_udp[start_index, Feature.BYTES] = sum_bytes
        data_udp[start_index, Feature.LAST_SWITCHED_TS] = max_ls
        duration = ((max_ls - data_udp[start_index, Feature.FIRST_SWITCHED_TS]) - time) / 1000
        data_udp[start_index, Feature.DURATION] = duration
        if duration > 0: 
          data_udp[start_index, Feature.BIT_RATE] = 8 * sum_bytes / duration
        # endregion
        # region ------------------------------------------------------------------------- 5. append result to aggregated flow
        aggregated_data.append(data_udp[start_index])
        start_index = stop_index
        # endregion

    def tcp(data_tcp):
      ''' aggregate tcp flow records for a unique tcp-5-tuple

        0. filter flow records based on export host (first appearance) 
        1. calculate time differences between flow records -> time_diff
        2. determine connection ends based on TCP flags (FIN or RST) -> stop indices
        for each stop index:
          3. drop flows without a defined start (SYN flag)
          4. calculate time gaps between flow records -> time
          5. combine properties of all flow records: sum of #packets/#bytes, duration (minus time) and bit rate, combine all TCP flags
          7. append result to aggregated flows (flow entries)

      @param data_tcp: flow records for a unique tcp-5-tuple (np.array)
      '''
      # region -------------------------------------------- 0. filter flow records based on export host (first appearance)
      # select flow records based on export host from first record, other records are dropped
      data_tcp = data_tcp[data_tcp[:, Feature.EXPORT_HOST] == data_tcp[0, Feature.EXPORT_HOST]]
      # endregion

      # region ---------------------------------------------------------- 1. calculate time differences between flow records
      time_diff = data_tcp[:, [Feature.FIRST_SWITCHED_TS, Feature.LAST_SWITCHED_TS]]  # get only first and last switched timestamp
      time_diff = time_diff.reshape(-1, 1)  # reshape to one column
      time_diff = np.squeeze(time_diff)  # remove "empty" dimension
      time_diff = np.diff(time_diff)  # calculate differences: ls_n - fs_n, fs_n+1 - ls_n
      # endregion
      # region ---------------------------------------- 2. determine connection ends based on TCP flags (FIN or RST)
      tcp_flags = data_tcp[:, Feature.TCP_FLAGS]  # load only TCP flags
      stop_indices = np.where(np.bitwise_and(tcp_flags, TCPFlags.FIN + TCPFlags.RST))  # find all stop indices based on TCP flags (FIN=1, RST=4)
      stop_indices = stop_indices[0]  # get first record with FIN or RST flag
      stop_indices = stop_indices.tolist()  # convert to index list
      start_index = 0  # init start index
      # endregion
      for stop_index in stop_indices:
        # region -------------------------------------------------- 3. drop flows without a defined start (SYN flag)
        # discard TCP flows where the first record does not contain a SYN flag
        if np.bitwise_and(data_tcp[start_index, Feature.TCP_FLAGS], TCPFlags.SYN) != TCPFlags.SYN:  # SYN=2
          start_index = stop_index + 1
          continue
        # endregion
        stop_index += 1  # increase stop index because of exclusive element selection
        # region ----------------------------------------------------------- 4. calculate time gaps between flow records
        time_diff_slice = time_diff[start_index * 2: (stop_index - 1) * 2]  # select all calculated time differences between records
        i1 = np.where(time_diff_slice > 0)[0]  # select all time differences greater than zero
        i2 = i1[i1 % 2 == 1]  # select all time differences between records
        time = np.sum(time_diff_slice[i2])  # sum of all time differences
        if not time: 
          time = 0.  # if no time differences exist, set time to zero
        # endregion
        # region --- 5. combine properties of all flow records: sum of #packets/#bytes, duration (minus time) and bit rate, combine all TCP flags
        sum_packets = np.sum(data_tcp[start_index: stop_index, Feature.PACKETS])  # sum #packets from start to stop index
        sum_bytes = np.sum(data_tcp[start_index: stop_index, Feature.BYTES])  # sum #bytes from start to stop index
        max_ls = np.max(data_tcp[start_index: stop_index, Feature.LAST_SWITCHED_TS])  # find max last switched time stamp
        tcp_flags = np.bitwise_or.reduce(data_tcp[start_index: stop_index, Feature.TCP_FLAGS], axis=0)  # combine TCP flags from all records

        data_tcp[start_index, Feature.PACKETS] = sum_packets  # update #packets
        data_tcp[start_index, Feature.BYTES] = sum_bytes  # update #bytes 
        data_tcp[start_index, Feature.TCP_FLAGS] = tcp_flags  # update TCP flags
        data_tcp[start_index, Feature.LAST_SWITCHED_TS] = max_ls  # update last switched timestamp
        duration = ((max_ls - data_tcp[start_index, Feature.FIRST_SWITCHED_TS]) - time) / 1000  # duration = last switched - first switched - time differences (seconds)
        data_tcp[start_index, Feature.DURATION] = duration  # update duration
        if duration > 0:
          data_tcp[start_index, Feature.BIT_RATE] = 8 * sum_bytes / duration
        # endregion
        # region ------------------------------------------------------------------------- 7. append aggregated flow
        aggregated_data.append(data_tcp[start_index])
        start_index = stop_index
        # endregion
    ##### ----------------------------

    # endregion
    
    # 3. combine all flow records with the same 5-tuple:
    # region ---------------------------------------------------------------- 3a. if flow records belong to TCP see tcp()
    if data[i, Feature.PROTOCOL] == 6:  
      tcp(equal_5tuples)
    # endregion
    # region ---------------------------------------------------------------- 3b. if flow records belong to UDP see udp()
    elif data[i, Feature.PROTOCOL] == 17: 
      udp(equal_5tuples)
    # endregion
    # region ------------------------------------------------------------------------------- 3c. neither TCP nor UDP
    else: 
      aggregated_data += equal_5tuples.tolist()
    # endregion

  data = np.array(aggregated_data)
  return data


@measure_time_memory
def enrich(data):
  ''' enrich flows with local and global topology information for source and destination ip address
  
  @param data: aggregated flow records/flow entries (np.array)
  @return enriched flow entries
  
  https://github.com/maxmind/MaxMind-DB-Reader-python 
  '''

  def lookup(ip_address):
    ''' determination/lookup of internal and external enrichment information
    
    @param ip_address: ip address (IPv4Address)
    @return enriched flow entries (list)
    '''
    enrich_fields = []
    if ip_address.is_private:
      enrich_fields += geo.hsfd_geo_data
      prefix, vlan = prefix_lookup.get_prefix_for_ip_private(str(ip_address))
    else:
      enrich_fields += geo.get_geo_information(ip_address)
      prefix, vlan = prefix_lookup.get_prefix_for_ip_public(str(ip_address))

    enrich_fields += [
      IPv4Address(prefix.network_address),  # prefix
      prefix.prefixlen,  # prefix length
      vlan if vlan else 0,  # vlan number
      1 if ip_address.is_private else 0,  # locality
      ]
    return enrich_fields

  lookup_fn = np.vectorize(lookup)
  lookup_src = lookup_fn(data[:, Feature.SRC_ADDR])  # source ip 
  lookup_dst = lookup_fn(data[:, Feature.DST_ADDR])  # destination ip 
  lookup_src = np.asarray(lookup_src.tolist(), dtype=object)
  lookup_dst = np.asarray(lookup_dst.tolist(), dtype=object)

  return np.concatenate((data, lookup_src, lookup_dst), axis=1)


@measure_time_memory
def format_(data):
  ''' format flow entries

  @param data: input flow data (np.array)
  @return: flow data with splitted first switched timestamp (np.array)
  '''

  def split_timestamp(timestamp):
    ''' split a timestamp (without milliseconds)

    @param timestamp: timestamp (milliseconds)
    @return: timestamp components (list(year, month, day, hour, minute, second))
    '''
    timestamp = datetime.utcfromtimestamp(timestamp // 1000)
    timestamp = [
      timestamp.year,
      timestamp.month,
      timestamp.day,
      timestamp.hour,
      timestamp.minute,
      timestamp.second,
      ]
    return np.array(timestamp, dtype=np.object)

  timestamp_fn = np.vectorize(split_timestamp)
  timestamp = timestamp_fn(data[:, Feature.FIRST_SWITCHED_TS])
  timestamp = np.asarray(timestamp.tolist(), dtype=object)
  return np.concatenate((data, timestamp), axis=1)


@measure_time_memory
def replace_randoms(data):
  ''' set randomly chosen port numbers from the operating system (> 32767) to 0
    PORT_MAX_LIMIT: threshold (if -1 port substitution is disabled)

    @param data: input flow data (np.array)
    @return: flow data with substituted port numbers (np.array)
  '''
  if PORT_MAX_LIMIT == -1: return data
  src_mask = data[:, Feature.SRC_PORT] >= PORT_MAX_LIMIT
  data[src_mask, Feature.SRC_PORT] = 0

  dst_mask = data[:, Feature.DST_PORT] >= PORT_MAX_LIMIT
  data[dst_mask, Feature.DST_PORT] = 0
  return data


@measure_time_memory
def anonymize(data):
  '''
  anonymize flow entries (see source/destination address/network)
  
  @param data: input flow data (np.array)
  @return: anonymized flow data with substituted ip addresses (np.array)  
  '''

  def convert_ips(ips):
    '''
    convert ip addresses based on substitution tables
    
    @param ips: ip addresses (np.array)
    @return converted ip addresses (np.array)
    '''    
    ips = ips.astype(np.uint32)  # convert from IPv4Address to int32
    split_oct_type = np.dtype((np.int32, {  # define split data type (individual octets)
      'oct0':(np.uint8, 3),
      'oct1':(np.uint8, 2),
      'oct2':(np.uint8, 1),
      'oct3':(np.uint8, 0),
      }))
    ips_octets = ips.view(dtype=split_oct_type)  # apply split data type

    # substitute each octet with the help of an individual substitution table 
    oct3 = SUBSTITUTION_TABLES[0][ips_octets['oct0']]
    oct2 = SUBSTITUTION_TABLES[1][ips_octets['oct1']]
    oct1 = SUBSTITUTION_TABLES[2][ips_octets['oct2']]
    oct0 = SUBSTITUTION_TABLES[3][ips_octets['oct3']]

    # reverse type conversions
    converted_ips = np.array([oct0, oct1, oct2, oct3]).T
    converted_ips = converted_ips.reshape(-1).view(dtype=np.uint32)
    converted_ips = np.vectorize(IPv4Address, otypes=[IPv4Address])(converted_ips)
    return converted_ips

  data[:, Feature.SRC_ADDR] = convert_ips(data[:, Feature.SRC_ADDR])  # source ip
  data[:, Feature.DST_ADDR] = convert_ips(data[:, Feature.DST_ADDR])  # destination ip
  data[:, Feature.SRC_NETWORK] = convert_ips(data[:, Feature.SRC_NETWORK])  # source network
  data[:, Feature.DST_NETWORK] = convert_ips(data[:, Feature.DST_NETWORK])  # destination network

  return data


pool = multiprocessing.Pool(NUM_PROCESSES)  # process pool for preprocessing of a data block


class Flow_Processor(Thread):

  def __init__(self, data, start, export=True):
    ''' processing class that uses a process pool to distribute a data block as chunks and collect the results of the executed preprocessing steps
        (side: flow data streaming server)
    @param data: input flow data block to process (list)
    @param start: start timestamp of the collection for the data block in seconds (float)
    @param export: indicator for whether the flow data should also be stored as a dataset (bool)
    '''    
    Thread.__init__(self)
    self.start_ = start
    self.end = time.time()

    self.data = np.array(data)
    self.num_elements = 0
    self.export = export

    if export:  # store raw dataset
      data = {
        'data': data,
        'properties': {
          'start': self.start_,
          'end':   self.end,
          }
        }

      self._pickle_data(data, 'raw_data.pkl.gz')

  def run(self):
    ''' start the preprocessing of a flow data block

      * split the data and distribute it to the processes in the process pool
      * wait until all processes are finished and combine the results
      * signal that data should be sent to the client(s)
    '''    
    log.debug('start Flow_Processor')
    start_process_block = time.time()
    data = self.data

    log.debug('number_of_elements_before_filtering: {}'.format(data.shape[0]))
    data = filter_features(data, feature_filter)
    log.debug('number_of_elements_after_filtering: {}'.format(data.shape[0]))

    # sort data by first switched timestamp (stable sort)
    data = data[data[:, Feature.FIRST_SWITCHED_TS].argsort(kind='mergesort')]
    data_ = data[:, FIVE_TUPLE].astype(np.uint32)  # 5-tuples 
    # find and sort all unique 5-tuples
    _, unique_5tuples_indices = np.unique(data_, return_index=True, axis=0)
    unique_5tuples_indices = np.sort(unique_5tuples_indices)

    # split data -> data chunks
    unique_5tuples_indices_slices = np.array_split(unique_5tuples_indices, NUM_PROCESSES)
    
    # distribute the data chunks to the processes
    args = [ (pid_, unique_5tuples_indices_slice, data, data_) for pid_, unique_5tuples_indices_slice in enumerate(unique_5tuples_indices_slices) ]
    log.debug('number_of_flows_before_aggregation: {}'.format(len(data)))
    global pool
    pool.map(foo, args)

    # collect and combine the results
    results = {}
    for _ in range(result_queue.qsize()):
      element = result_queue.get()
      results[element[0]] = element[1]
    results = [ results[id_] for id_ in range(NUM_PROCESSES) ]
    data = np.concatenate(results, axis=0)
    
    end_process_block = time.time()
    log.debug('process_time_block: {}s'.format(end_process_block - start_process_block))
    log.debug('number_of_flows_after_aggregation: {}'.format(len(data)))

    # enqueue data, wake up all client threads
    CH.enqueue(data)
    CH.wakeup()

    if self.export:  # store preprocessed dataset
      data = {
        'data': data,
        'properties': {
          'start_collect': self.start_,
          'end_collect':   self.end,
          'start_process': start_process_block,
          'end_process': end_process_block,
        }
      }

      self._pickle_data(data, 'data.pkl.gz')
    log.debug('end Flow_Processor')

  def _pickle_data(self, data, filename):
    ''' write data to pickle file '''
    with gzip.GzipFile(filename, 'ab' if os.path.isfile(filename) else 'wb') as file:
      pickle.dump(data, file, pickle.HIGHEST_PROTOCOL)
