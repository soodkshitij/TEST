import grpc
import server_pb2_grpc
import server_pb2
import config
from sys import argv
import logger as lg
import time

logger = lg.get_logger()

heartbeat_interval =2

class Client():
    
    def __init__(self, host, port):
        self.channel = grpc.insecure_channel('%s:%d' % (host, port))
        self.stub = server_pb2_grpc.CommunicationServiceStub(self.channel)
        self.port = port
        
    def getClientStatus(self, requested_by=0):
        req = (server_pb2.ReplicationRequest(id=requested_by))
        return self.stub.getClientStatus(req)
    
    def ping(self):
        return self.stub.ping(server_pb2.LeaderRequest(data="empty"))
    
    def setLeader(self,leader_id):
        return self.stub.setLeader(server_pb2.ReplicationRequest(id=leader_id))
    
    def requestVote(self, node_id):
        return self.stub.requestVote(server_pb2.ReplicationRequest(id=node_id))
    
    def getLeaderNode(self, node_id):
        return self.stub.getLeaderNode(server_pb2.ReplicationRequest(id=node_id))
    
    def GetHandler(self, from_timestamp, to_timestamp):
        req = server_pb2.Request(
            fromSender='some put sender',
            toReceiver='some put receiver',
        getRequest=server_pb2.GetRequest(
          metaData=server_pb2.MetaData(uuid='14829'),
          queryParams=server_pb2.QueryParams(from_utc=from_timestamp,to_utc=to_timestamp))
        )
        for stream in self.stub.GetHandler(req):
            yield(stream)
            
    def GetFromLocalCluster(self, from_timestamp, to_timestamp):
        req = server_pb2.Request(
            fromSender='some put sender',
            toReceiver='some put receiver',
        getRequest=server_pb2.GetRequest(
          metaData=server_pb2.MetaData(uuid='14829'),
          queryParams=server_pb2.QueryParams(from_utc=from_timestamp,to_utc=to_timestamp))
        )
        print("Client GetFromLocalCluster",req)
        for stream in self.stub.GetFromLocalCluster(req):
            yield(stream)
        
    def PutHandler(self, data):
        pass


def run():
    config.populate()
    node_id = config.get_node_id()
    node_details = config.get_node_details(node_id)
    logger.info("Connecting to host {} on port {}".format(node_details[0], node_details[1]))
    c = Client(node_details[0],node_details[1])
    leader_node = 0
    while(True):
        time.sleep(heartbeat_interval)
        leader = c.getLeaderNode(node_id)
        if leader_node != leader.id and leader.id == node_id:
            leader_node = leader.id
            logger.info("Publish node_id {} to external cluster".format(node_id))


if __name__ == "__main__":
    run()