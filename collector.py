'''
flow collector (NetFlow v5)
'''
import socket, select, struct
from ipaddress import IPv4Address
from processor import Flow_Processor
from connection_handler import Connection_Handler as CH
from connection_handler import client_threads
from log import log
from command_line_interpreter import Command_Line_Interpreter as CLI
from constants import *
from utils import debug_flow
import time

# filtering flow data that matches specified feature values (lambda expressions)
feature_filter = {
	# e.g., excluding DNS traffic
	# (6,): lambda x: x == 53,
	# (7,): lambda x: x == 53,
  # (6,7): lambda x,y: x == 53 or y == 53,
  }

start = None


class NetFlowPacket():
	HEADER_STRUCTURE = '!HHIIIIBBH'
	HEADER_LENGTH = struct.calcsize(HEADER_STRUCTURE)

	FLOW_STRUCTURE = '!IIIHHIIIIHHBBBBHHBBH'
	FLOW_LENGTH = struct.calcsize(FLOW_STRUCTURE)

	FLOWS = []
	flow_counter = 0
	current_flow_counter = 0
	drop_counter = 0
	filter_counter = 0
	block_counter = 1

	def __init__(self, data, exporter):
		''' class that represents a NetFlow packet and its handling by the collector 
		
		@param data: received NetFlow packet/data
		@param exporter: address of the NetFlow exporter
		'''
		header_fields = struct.unpack(self.HEADER_STRUCTURE, data[:self.HEADER_LENGTH])
		# self.version = header_fields[0]
		self.count = header_fields[1]  # number of individual flow records in the NetFlow packet

		global start  # start timestamp for the collection of a flow data block 
		if not start: 
			start = time.time()

		for n in range(self.count):
			# extract flow information
			offset = self.HEADER_LENGTH + (self.FLOW_LENGTH * n)
			flow_data = data[offset:offset + self.FLOW_LENGTH]
			flow_fields = struct.unpack(self.FLOW_STRUCTURE, flow_data)

			sys_start = header_fields[3] * 1000 - header_fields[2]  # system start timestamp in ms (local exporter)

			# precalculate duration and bit rate (recalculated for aggregated flows)
			duration = (flow_fields[8] - flow_fields[7]) / 1000  # in seconds
			bit_rate = 0 if duration == 0 else (flow_fields[6] * 8) / duration  # bits per second

			flow = [
				IPv4Address(flow_fields[0]),  # 0  src_addr
				IPv4Address(flow_fields[1]),  # 1  dst_addr
			  flow_fields[5],  # 2  num_packets
				flow_fields[6],  # 3  num_bytes
				sys_start + flow_fields[7],  # 4  first_switched
				sys_start + flow_fields[8],  # 5  last_switched
				flow_fields[9],  # 6  src_port
				flow_fields[10],  # 7  dst_port
				flow_fields[12],  # 8  tcp_flags
				flow_fields[13],  # 9  protocol
				exporter[0],  # 10 exporter
				NetFlowPacket.flow_counter,  # 11 flow_counter
				duration,  # 12 duration
				bit_rate,  # 13 bit_rate
				]

			# filter/drop multicasts, all-broadcasts and flows with reserved ip addresses
			if (flow[0].is_multicast or
					flow[0].is_reserved or
					flow[0] == IPv4Address('255.255.255.255') or
					flow[1].is_multicast or
					flow[1].is_reserved or
					flow[1] == IPv4Address('255.255.255.255')
					):
				NetFlowPacket.drop_counter += 1
				continue

			try:  # apply the filter expressions to collected flow data
				for feature_key, lambda_exp in feature_filter.items():
					if lambda_exp(*[flow[x] for x in feature_key]):
						raise Exception
			except Exception:
				NetFlowPacket.filter_counter += 1
				continue

			# debug_flow(flow, src_addr=None, dst_addr='192.168.42.XX', src_port=None, dst_port=1337)

			NetFlowPacket.flow_counter += 1
			self.FLOWS.append(flow)

		if len(NetFlowPacket.FLOWS) >= BLOCK_SIZE:  # initiate the preprocessing for a flow data block
			log.info('start process {} flows ({})'.format(len(NetFlowPacket.FLOWS), time.time()))
			Flow_Processor(NetFlowPacket.FLOWS, start).start()
			NetFlowPacket.FLOWS = []
			NetFlowPacket.flow_counter = 0
			log.debug('drop_counter: {}'.format(NetFlowPacket.drop_counter))
			NetFlowPacket.drop_counter = 0
			log.debug('filter_counter: {}'.format(NetFlowPacket.filter_counter))
			NetFlowPacket.filter_counter = 0
			log.debug('block_counter: {}'.format(NetFlowPacket.block_counter))
			NetFlowPacket.block_counter += 1
			start = None


log.info('start command line interpreter')
command_line_interpreter = CLI(None)
command_line_interpreter.start()

log.info('start connection handler')
connection_handler = CH()
connection_handler.start()

log.debug('bind socket for {}:{} with block size = {}'.format(HOST, COLLECTOR_PORT, BLOCK_SIZE))
socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
socket.bind((HOST, COLLECTOR_PORT))

log.info('start NetFlow collector loop')
while True:  # loop for receiving exported flow records (NetFlow messages)
	rlist = select.select([socket], [], [])[0]
	(data, exporter) = rlist[0].recvfrom(BUFFER_SIZE)
	if len(client_threads) == 0:  # only process NetFlow messages if at least one client is connected 
		continue
	NetFlowPacket(data, exporter)
