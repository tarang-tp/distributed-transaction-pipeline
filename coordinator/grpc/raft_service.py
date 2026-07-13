"""gRPC server side: turns incoming unary RPCs into direct, synchronous
calls against a RaftNodeRuntime -- NOT routed through
RaftNode.receive()/Transport.send(), which model outbound, fire-and-forget
messaging. A gRPC unary call already IS a synchronous request/response
pair, so the reply is just this handler's return value.
"""

from __future__ import annotations

import pickle

from coordinator.grpc import raft_pb2, raft_pb2_grpc, serialization
from coordinator.raft_runtime import RaftNodeRuntime


class RaftGrpcServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, runtime: RaftNodeRuntime):
        self._runtime = runtime

    def RequestVote(self, request, context):
        args = serialization.request_vote_args_from_proto(request)
        reply = self._runtime.handle_request_vote(args)
        return serialization.request_vote_reply_to_proto(reply)

    def PreVote(self, request, context):
        args = serialization.prevote_args_from_proto(request)
        reply = self._runtime.handle_prevote(args)
        return serialization.prevote_reply_to_proto(reply)

    def AppendEntries(self, request, context):
        args = serialization.append_entries_args_from_proto(request)
        reply = self._runtime.handle_append_entries(args)
        return serialization.append_entries_reply_to_proto(reply)

    def Propose(self, request, context):
        command = pickle.loads(request.command)
        entry = self._runtime.propose(command)
        if entry is None:
            return raft_pb2.ProposeReply(accepted=False, leader_hint=self._runtime.leader_id or "")
        return raft_pb2.ProposeReply(
            accepted=True, term=entry.term, index=entry.index, leader_hint=self._runtime.node_id
        )
