from config import get_client_map, get_node_details, populate
from client import Client
populate()
node_details = get_node_details(1)
print(node_details)
c = Client(node_details[0],node_details[1])
#c = Client('169.254.208.33',000)
import server_pb2
import server_pb2_grpc
from server_pb2 import Request, GetRequest, QueryParams 

req = Request(fromSender="",toReceiver="",getRequest = GetRequest(queryParams=QueryParams(from_utc="2012-01-01",to_utc="2020-01-01")))
count = 0
for x in (c.getHandler('2012-01-01','2020-01-01')):
    count+=1
    print ("count ",count)
    print(x)


# res = c.ping("hello")
# print (res)

#print(c.GetHandler(1, 2))