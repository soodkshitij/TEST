from config import get_client_map, get_node_details, populate
from client import Client
populate()
node_details = get_node_details(1)
print(node_details)
c = Client(node_details[0],node_details[1])
import server_pb2
import server_pb2_grpc
from server_pb2 import Request, GetRequest, QueryParams 

#req = Request(fromSender="",toSender="",getRequest = GetRequest(queryParams=QueryParams(from_utc="2012-01-01",to_utc="2020-01-01"))

for x in (c.GetHandler('2020-01-01','2020-01-01')):
     print(x)

#print(c.GetHandler(1, 2))