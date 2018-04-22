import glob
import gzip
import shutil
import re

from chunktest import process
from config import get_client_map, get_node_details, populate
from client import Client
import server_pb2
import server_pb2_grpc
from server_pb2 import Request, PutRequest, QueryParams, DatFragment

populate()
c = Client('127.0.0.1',3000)

list_of_files = glob.glob('./data/*.gz')
for file_name in list_of_files:
	with gzip.open(file_name, 'rb') as f_in:
		with open(file_name[:-3], 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)

# Mesonet CDFNet file format for now just imagine that the parser did its job and we have CSV format
list_of_files = glob.glob('./data/*')
for file_name in list_of_files:
	if file_name.endswith('.out'):
		# mesowest format ending with .out
		c.process(open(file_name))
	elif '_' in file_name and file_name.endswith('.csv'):
		# Assuming Mesonet has <date>_<time> format
		c.process(open(file_name))
	else:
		print("Nothing do for this file")
