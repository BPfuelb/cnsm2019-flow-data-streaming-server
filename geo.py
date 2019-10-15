'''
download the latest version of the geo databases and load them
'''
import geoip2.database
import urllib.request 
import hashlib
import os
import shutil
import re
import pathlib
import tarfile
import zipfile

# region --------------------------------------------------------------------------------------  geo database parameters
# considered geo features
GEO_KEYS = ['country_code', 'postal', 'longitude', 'latitude', 'asn']

# GEO_KEYS = ['country_code', 'country_name', 'postal',
#             'city', 'longitude', 'latitude', 'asn', 'organisation']

# locally stored data sources for the enrichment (local and global topology)
GEO_DATA = {
  'country':{
      'db_file':'./db/GeoLite2-Country/GeoLite2-Country.mmdb',
      'db_file_dir':'./db/GeoLite2-Country/',
      'db_zip_file_local':'./db/GeoLite2-Country.tar.gz',
      'db_zip_file_remote':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz',
      'db_zip_file_md5':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz.md5'        
    },
  'city':{
      'db_file':'./db/GeoLite2-City/GeoLite2-City.mmdb',
      'db_file_dir':'./db/GeoLite2-City/',
      'db_zip_file_local':'./db/GeoLite2-City.tar.gz',
      'db_zip_file_remote':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz',
      'db_zip_file_md5':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz.md5'      
    },
  'asn':{
    'db_file':'./db/GeoLite2-ASN/GeoLite2-ASN.mmdb',
    'db_file_dir':'./db/GeoLite2-ASN/',
    'db_zip_file_local':'./db/GeoLite2-ASN.tar.gz',
    'db_zip_file_remote':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz',
    'db_zip_file_md5':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz.md5'      
    },
  'public_prefixes':{
    'db_file':'./db/GeoLite2-ASN-CSV/GeoLite2-ASN-Blocks-IPv4.csv',
    'db_file_dir':'./db/GeoLite2-ASN-CSV/',
    'db_zip_file_local':'./db/GeoLite2-ASN-CSV.zip',
    'db_zip_file_remote':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN-CSV.zip',
    'db_zip_file_md5':'http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN-CSV.zip.md5'    
    },
  'private_prefixes_file': './db/private_prefixes.csv',
  'private_prefixes_vlans': './db/private_prefixes_vlans.csv',
  'public_prefixes_lookup_file':'./db/public_prefixes_lookup_tree.pkl.gz'
  }
# endregion
  
country_reader, city_reader, asn_reader = None, None, None
hsfd_geo_data = None

NEW_PREFIXES = False

country_abbreviations = [
          None,
          'AF', 'AX', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG',  # 10 per line
          'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD', 'BB',
          'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW',
          'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'CV', 'KH', 'CM',
          'CA', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM',
          'CD', 'CG', 'CK', 'CR', 'CI', 'HR', 'CU', 'CW', 'CY', 'CZ',
          'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE',
          'SZ', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF',
          'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP',
          'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN',
          'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL',
          'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR',
          'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT',
          'LU', 'MO', 'MK', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH',
          'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME',
          'MS', 'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'NC', 'NZ',
          'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW',
          'PS', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR',
          'QA', 'RE', 'RO', 'RU', 'RW', 'BL', 'SH', 'KN', 'LC', 'MF',
          'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL',
          'SG', 'SX', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES',
          'LK', 'SD', 'SR', 'SJ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ',
          'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC',
          'TV', 'UG', 'UA', 'AE', 'GB', 'UM', 'US', 'UY', 'UZ', 'VU',
          'VE', 'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW',
          ]
country_lookup_dict = { abbrev: id_ for id_, abbrev in enumerate(country_abbreviations) }
country_lookup_dict.update({'None':0})  # None and 'None' -> 0


def load_data():
  ''' load the latest version of each geo database (ASN, city, country) '''  
  try:
    os.mkdir('./db')
  except FileExistsError:
    pass
  
  for _type, data in GEO_DATA.items():
    
    if type(data) is str: 
      continue
    
    if not check_remote_database(data['db_zip_file_local'], data['db_zip_file_md5']):
      if os.path.isfile(data['db_file']):
        os.remove(data['db_file'])
      if _type == 'public_prefixes' and os.path.isfile(GEO_DATA['public_prefixes_lookup_file']):  
        os.remove(GEO_DATA['public_prefixes_lookup_file'])
      with urllib.request.urlopen(data['db_zip_file_remote']) as response, open(data['db_zip_file_local'], 'wb') as output:
        download = response.read()
        output.write(download)
      if _type == 'public_prefixes':
        global NEW_PREFIXES
        NEW_PREFIXES = True

      extension = ''.join(pathlib.Path(data['db_zip_file_local']).suffixes)
      if extension == '.tar.gz':
        with tarfile.open(data['db_zip_file_local'], 'r:*') as tar_gz_file:    
          member = [x for x in tar_gz_file.getmembers() if re.compile('^.*.mmdb$').match(x.name)][0]
          tar_gz_file.extractall(data['db_file_dir'], [member])
          shutil.move(data['db_file_dir'] + member.name, data['db_file_dir'])
          old_directory = data['db_file_dir'] + member.name.split('/')[0]
          if not os.listdir(old_directory):
            os.rmdir(old_directory)
      elif extension == '.zip':
        with zipfile.ZipFile(data['db_zip_file_local'], 'r') as zip_file:
          member = [x for x in zip_file.namelist() if re.compile('^.*IPv4.csv$').match(x)][0]
          zip_file.extractall(data['db_file_dir'], [member])
          shutil.move(data['db_file_dir'] + member, data['db_file_dir'])
          old_directory = data['db_file_dir'] + member.split('/')[0]
          if not os.listdir(old_directory):
            os.rmdir(old_directory)       
  
  # create an individual database reader for each database file
  global country_reader, city_reader, asn_reader
  ''' https://github.com/maxmind/MaxMind-DB-Reader-python '''
  country_reader = geoip2.database.Reader(GEO_DATA['country']['db_file'])
  city_reader = geoip2.database.Reader(GEO_DATA['city']['db_file'])
  asn_reader = geoip2.database.Reader(GEO_DATA['asn']['db_file'])
  
  global hsfd_geo_data
  hsfd_geo_data = get_geo_information(get_external_ip())


def check_remote_database(local_file, remote_url):
  ''' check for an updated version of the local databases based on the md5 hash values
  
  @param local_file: path to the local database file (str)
  @param remote_url: url to the most recently published database (str)
  @return result of the check for actuality (bool)
  '''  
  if os.path.isfile(local_file):
    md5_local = hashlib.md5(open(local_file, 'rb').read()).hexdigest()
  else: 
    md5_local = -1

  md5_remote = urllib.request.urlopen(remote_url).read().decode()    
  
  return md5_local == md5_remote 


def get_geo_information(ip_address):
  ''' retrieve geo information, None values are replaced with numerical zero values
  
  @param ip_address: ip address for which the geo information is retrieved (IPv4Address)
  @return retrieved geo information (dict)  
  '''  
  try:
    country_response = country_reader.country(str(ip_address))  
    city_response = city_reader.city(str(ip_address))
    asn_response = asn_reader.asn(str(ip_address))
  except:
    return [0,  # country_code
            # 'None', # postal
            0.,  # longitude
            0.,  # latitude
            0,  # asn
            ]

  def __if_None(val, alt):
    ''' replace None values with an alternative value
    
    @param val: queried database value
    @param alt: alternative value to be used for the replacement
    @return original or altered value (if None)
    '''    
    if val is None: return alt
    return val
  
  return [country_lookup_dict.get(country_response.country.iso_code, 0),  # country_code
          # __if_None(city_response.postal.code, 'None'),                # postal
          __if_None(city_response.location.latitude, 0.),  # longitude
          __if_None(city_response.location.longitude, 0.),  # latitude
          __if_None(asn_response.autonomous_system_number, 0),  # asn
          ]


def get_external_ip():
  ''' get the external/public ip address for the local system
  
  @return external/public ip address
  '''    
  result = urllib.request.urlopen('http://checkip.dyndns.org').read().decode()
  return re.findall(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', result)[0]
