''' 
organize/manage lookup trees for private and public prefixes as well as vlan information 
'''

import numpy as np
from ipaddress import IPv4Network
import geo
import utils
import os

# temporary loaded csv data for public prefixes
public_prefixes = None
# temporary loaded csv data for private prefixes
private_prefixes = None

# lookup tree for public prefixes 
prefix_lookup_public = [None, None]
# lookup tree for private prefixes
prefix_lookup_private = [None, None]


def load_prefix_data():
  '''  build and load the lookup trees for private and public prefixes  '''
  global private_prefixes, public_prefixes, prefix_lookup_public
  
  # region ---------------------------------------------------------------------------- build public prefixes lookup tree  
  if geo.NEW_PREFIXES is False and os.path.isfile(utils.PICKLE_FILE_PREFIXES):
    # load existing pickled lookup tree for public prefixes
    prefix_lookup_public = utils.load_pickle_file(utils.PICKLE_FILE_PREFIXES)
  else:  # build (updated) lookup tree for public prefixes
    if os.path.isfile(utils.PICKLE_FILE_PREFIXES):
      os.remove(utils.PICKLE_FILE_PREFIXES)

    def _select(data):
      ''' select prefix information from a loaded csv line (e.g., 8.8.4.0/24,15169,"Google LLC")
      
      @param data: csv line (str)
      @return ip prefix (str)
      '''
      return data.split(',', 1)[0]

    # load public prefix information from csv file
    public_prefixes = utils.load_csv_file(geo.GEO_DATA['public_prefixes']['db_file'], _select=_select, skip_header=True)
    # build lookup tree
    __build_prefix_lookup_public()
    # endregion
    
  # region --------------------------------------------------------------------------- build private prefixes lookup tree    
  def _filter_host_addresses(data):
    ''' check if the prefix is a host prefix (prefix length /32)
    
    @param data: ip prefix (str)
    @return result of the check (bool)
    '''      
    return data.strip().split('/')[1] == '32'

  # load private prefix information from csv file
  private_prefixes = utils.load_csv_file(geo.GEO_DATA['private_prefixes_file'], _filter=_filter_host_addresses)
  
  def _filter_not_available(data):
    ''' check if VLAN information is not available (N/A)
    
    @param data: VLAN information (str)
    @return result of the check (bool)
    '''    
    return data.strip().split(',')[1] == 'N/A'

  # load vlan information from csv file and build mapping between a private prefix and a VLAN  
  vlans = utils.load_csv_file(geo.GEO_DATA['private_prefixes_vlans'], _filter=_filter_not_available, skip_header=True)
  vlans = { prefix: vlan for (prefix, vlan) in [ line.split(',', 1) for line in vlans ] }

  # build lookup tree  
  __build_prefix_lookup_private(vlans)
  # endregion

  # clear temporary loaded csv prefix data
  __clear_prefix_lists()


@utils.measure_time_memory
def __build_prefix_lookup_public():
  ''' create a lookup tree for public prefixes and pickle the result '''
  __build_prefix_lookup(public_prefixes, prefix_lookup_public)
  __replace_unknown_prefixes(prefix_lookup_public)
  utils.pickle_prefixes(prefix_lookup_public)


@utils.measure_time_memory                      
def __build_prefix_lookup_private(vlans):
  ''' create a lookup tree for private ip prefixes and do not pickle the result
  
  @param vlans: mapping between a private prefix and a VLAN (dict)
  '''   
  __build_prefix_lookup(private_prefixes, prefix_lookup_private, vlans)
  __replace_unknown_prefixes(prefix_lookup_private)

                      
def __build_prefix_lookup(prefixes, lookup, vlans=None):
  ''' recursively construct a prefix lookup tree and add an ip prefix
  
  @param prefixes: ip prefixes (list) 
  @param lookup: reference to the prefix lookup tree (prefix_lookup_private, prefix_lookup_public)
  @param vlans: mapping between a private prefix and a VLAN (dict)  
  '''  
  for _, prefix in enumerate(prefixes):
    ip, prefix_length = prefix.split('/', 1)

    def __add_prefix(parent, bit_mask, prefix_length, prefix):
      '''
      @param parent: current parent tree node (list or tuple) 
      @param bit_mask: bit mask of the ip prefix (str)
      @param prefix_length: ip prefix length (str)  
      @param prefix: ip prefix (str)
      '''       
      bit = int(bit_mask[0])
      new_network = IPv4Network(prefix)
      
      if type(parent[bit]) is tuple: 
        if parent[bit][0].prefixlen < new_network.prefixlen: 
          parent[bit] = [None, None]
          
      if prefix_length == 1:
        if vlans is not None:
          parent[bit] = (new_network, vlans.get(new_network.with_prefixlen, 0))
        else:
          parent[bit] = (new_network, 0)
        return
      
      if parent[bit] is None:
        parent[bit] = [None, None]
      
      __add_prefix(parent[bit], bit_mask[1:], prefix_length - 1, prefix)
  
    __add_prefix(lookup, ''.join([ np.binary_repr(int(x), width=8) for x in ip.split('.') ]), int(prefix_length), prefix)


def get_prefix_for_ip_public(ip):
  ''' determine the ip prefix for a public ip address
  
  @param ip: public ip address (str)
  @return public ip prefix and VLAN (str,0)
  '''  
  return __get_prefix_for_ip(ip, prefix_lookup_public)

                      
def get_prefix_for_ip_private(ip):
  ''' determine the ip prefix and VLAN information for a private ip address 
  
  @param ip: private ip address (str)
  @return private ip prefix and VLAN (str,int)
  '''  
  return __get_prefix_for_ip(ip, prefix_lookup_private)


def __get_prefix_for_ip(ip, lookup):
  ''' recursively get the ip prefix and VLAN information for an ip address from a prefix lookup tree
  
  @param ip: ip address (str)
  @param lookup: reference to the prefix lookup tree (prefix_lookup_private, prefix_lookup_public)
  @return ip prefix and VLAN (str,int)  
  '''  
  bit_mask = ''.join([ np.binary_repr(int(x), width=8) for x in ip.split('.') ])

  def __get_prefix(parent, bit_mask):
    '''
    @param parent: current parent tree node (list or tuple)
    @param bit_mask: bit mask of the ip address (str)
    @return ip prefix and VLAN (str,int)  
    '''    
    bit = int(bit_mask[0])
    if not isinstance(parent[bit], list):
      return parent[bit]
    return __get_prefix(parent[bit], bit_mask[1:])

  return __get_prefix(lookup, bit_mask)

  
def __clear_prefix_lists():
  ''' clear temporary loaded csv prefix data '''  
  global private_prefixes, public_prefixes
  del private_prefixes
  del public_prefixes


def __replace_unknown_prefixes(lookup):
  '''
  recursively replace leaf nodes with None values in the lookup tree by a default prefix
  
  @param lookup: reference to the prefix lookup tree (prefix_lookup_private, prefix_lookup_public)
  '''  
  default_prefix = (IPv4Network('0.0.0.0/0'), 0)
 
  def __replace_none_values(lookup):
    '''
    @param lookup: reference to the prefix lookup tree (current position in the tree)
    '''    
    if not isinstance(lookup[0], list):
      if lookup[0] is None:
        lookup[0] = default_prefix
    else:
      __replace_none_values(lookup[0])

    if not isinstance(lookup[1], list):
      if lookup[1] is None:
        lookup[1] = default_prefix
    else:
      __replace_none_values(lookup[1])
      
  __replace_none_values(lookup)
