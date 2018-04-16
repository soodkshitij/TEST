# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import server_pb2 as server__pb2


class CommunicationServiceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.pingInternal = channel.unary_unary(
        '/grpcComm.CommunicationService/pingInternal',
        request_serializer=server__pb2.LeaderRequest.SerializeToString,
        response_deserializer=server__pb2.BoolResponse.FromString,
        )
    self.getClientStatus = channel.unary_unary(
        '/grpcComm.CommunicationService/getClientStatus',
        request_serializer=server__pb2.ReplicationRequest.SerializeToString,
        response_deserializer=server__pb2.ClientResponse.FromString,
        )
    self.setLeader = channel.unary_unary(
        '/grpcComm.CommunicationService/setLeader',
        request_serializer=server__pb2.ReplicationRequest.SerializeToString,
        response_deserializer=server__pb2.BoolResponse.FromString,
        )
    self.requestVote = channel.unary_unary(
        '/grpcComm.CommunicationService/requestVote',
        request_serializer=server__pb2.ReplicationRequest.SerializeToString,
        response_deserializer=server__pb2.BoolResponse.FromString,
        )
    self.setNodeState = channel.unary_unary(
        '/grpcComm.CommunicationService/setNodeState',
        request_serializer=server__pb2.NodeState.SerializeToString,
        response_deserializer=server__pb2.BoolResponse.FromString,
        )
    self.getLeaderNode = channel.unary_unary(
        '/grpcComm.CommunicationService/getLeaderNode',
        request_serializer=server__pb2.ReplicationRequest.SerializeToString,
        response_deserializer=server__pb2.ReplicationRequest.FromString,
        )
    self.putHandler = channel.stream_unary(
        '/grpcComm.CommunicationService/putHandler',
        request_serializer=server__pb2.Request.SerializeToString,
        response_deserializer=server__pb2.Response.FromString,
        )
    self.getHandler = channel.unary_stream(
        '/grpcComm.CommunicationService/getHandler',
        request_serializer=server__pb2.Request.SerializeToString,
        response_deserializer=server__pb2.Response.FromString,
        )
    self.GetFromLocalCluster = channel.unary_stream(
        '/grpcComm.CommunicationService/GetFromLocalCluster',
        request_serializer=server__pb2.Request.SerializeToString,
        response_deserializer=server__pb2.Response.FromString,
        )
    self.PutToLocalCluster = channel.stream_unary(
        '/grpcComm.CommunicationService/PutToLocalCluster',
        request_serializer=server__pb2.Request.SerializeToString,
        response_deserializer=server__pb2.Response.FromString,
        )
    self.ping = channel.unary_unary(
        '/grpcComm.CommunicationService/ping',
        request_serializer=server__pb2.Request.SerializeToString,
        response_deserializer=server__pb2.Response.FromString,
        )


class CommunicationServiceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def pingInternal(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def getClientStatus(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def setLeader(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def requestVote(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def setNodeState(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def getLeaderNode(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def putHandler(self, request_iterator, context):
    """interface, for inter-intra cluster communication
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def getHandler(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetFromLocalCluster(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def PutToLocalCluster(self, request_iterator, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ping(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_CommunicationServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'pingInternal': grpc.unary_unary_rpc_method_handler(
          servicer.pingInternal,
          request_deserializer=server__pb2.LeaderRequest.FromString,
          response_serializer=server__pb2.BoolResponse.SerializeToString,
      ),
      'getClientStatus': grpc.unary_unary_rpc_method_handler(
          servicer.getClientStatus,
          request_deserializer=server__pb2.ReplicationRequest.FromString,
          response_serializer=server__pb2.ClientResponse.SerializeToString,
      ),
      'setLeader': grpc.unary_unary_rpc_method_handler(
          servicer.setLeader,
          request_deserializer=server__pb2.ReplicationRequest.FromString,
          response_serializer=server__pb2.BoolResponse.SerializeToString,
      ),
      'requestVote': grpc.unary_unary_rpc_method_handler(
          servicer.requestVote,
          request_deserializer=server__pb2.ReplicationRequest.FromString,
          response_serializer=server__pb2.BoolResponse.SerializeToString,
      ),
      'setNodeState': grpc.unary_unary_rpc_method_handler(
          servicer.setNodeState,
          request_deserializer=server__pb2.NodeState.FromString,
          response_serializer=server__pb2.BoolResponse.SerializeToString,
      ),
      'getLeaderNode': grpc.unary_unary_rpc_method_handler(
          servicer.getLeaderNode,
          request_deserializer=server__pb2.ReplicationRequest.FromString,
          response_serializer=server__pb2.ReplicationRequest.SerializeToString,
      ),
      'putHandler': grpc.stream_unary_rpc_method_handler(
          servicer.putHandler,
          request_deserializer=server__pb2.Request.FromString,
          response_serializer=server__pb2.Response.SerializeToString,
      ),
      'getHandler': grpc.unary_stream_rpc_method_handler(
          servicer.getHandler,
          request_deserializer=server__pb2.Request.FromString,
          response_serializer=server__pb2.Response.SerializeToString,
      ),
      'GetFromLocalCluster': grpc.unary_stream_rpc_method_handler(
          servicer.GetFromLocalCluster,
          request_deserializer=server__pb2.Request.FromString,
          response_serializer=server__pb2.Response.SerializeToString,
      ),
      'PutToLocalCluster': grpc.stream_unary_rpc_method_handler(
          servicer.PutToLocalCluster,
          request_deserializer=server__pb2.Request.FromString,
          response_serializer=server__pb2.Response.SerializeToString,
      ),
      'ping': grpc.unary_unary_rpc_method_handler(
          servicer.ping,
          request_deserializer=server__pb2.Request.FromString,
          response_serializer=server__pb2.Response.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'grpcComm.CommunicationService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
