"""Transport implementation backed by real gRPC calls between separate
processes -- implements raft/transport.py's Transport protocol so RaftNode
and RaftNodeRuntime need no changes to run over a real network instead of
raft/tests/harness's simulated one or the in-process InMemoryTransport.

Because gRPC unary RPCs are synchronous request/response, send() behaves
differently by message type:

- Outbound REQUESTS (RequestVoteArgs/PreVoteArgs/AppendEntriesArgs): a real
  blocking gRPC call is made on a background thread (so the driver loop
  calling send() never blocks), and the reply is fed back into the LOCAL
  node via runtime.receive() -- exactly as if it had arrived asynchronously
  through an inbox, which is how the in-memory transport models it too.
- Outbound REPLIES (RequestVoteReply/PreVoteReply/AppendEntriesReply): these
  only exist in the in-memory/simulated model, where "replying" means
  sending a new message. Over real gRPC the reply IS the return value of
  the RPC call the server is already inside of (raft_service.py), so
  there's nothing to send -- this is a no-op.

register()/deliver_to() are no-ops: nothing is queued locally to poll,
since incoming requests are handled directly by the gRPC server and
incoming replies are injected via runtime.receive() as they arrive.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import grpc

from coordinator.grpc import raft_pb2_grpc, serialization
from raft.transport import Envelope, Message
from raft.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    PreVoteArgs,
    PreVoteReply,
    RequestVoteArgs,
    RequestVoteReply,
)

logger = logging.getLogger(__name__)


class GrpcRaftTransport:
    def __init__(self, addresses: dict[str, str], call_timeout: float = 0.5, max_workers: int = 8):
        """addresses: node_id -> 'host:port' for every OTHER node in the cluster."""
        self._addresses = addresses
        self._call_timeout = call_timeout
        self._channels: dict[str, grpc.Channel] = {}
        self._stubs: dict[str, raft_pb2_grpc.RaftServiceStub] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="grpc-raft-send")
        self._self_node_id: str | None = None
        self._runtime = None

    def bind_runtime(self, runtime) -> None:
        """Wires this transport to the local node it serves. Needed because
        outbound-call replies must be injected back into THIS node, and the
        transport is constructed before the RaftNodeRuntime that owns it.
        """
        self._self_node_id = runtime.node_id
        self._runtime = runtime

    def register(self, node_id: str) -> None:
        pass  # addresses are provided up front; nothing to register

    def deliver_to(self, node_id: str, handler) -> int:
        return 0  # see module docstring: nothing is queued locally

    def close(self) -> None:
        self._executor.shutdown(wait=False)
        for channel in self._channels.values():
            channel.close()

    def _stub_for(self, node_id: str) -> raft_pb2_grpc.RaftServiceStub:
        stub = self._stubs.get(node_id)
        if stub is None:
            channel = grpc.insecure_channel(self._addresses[node_id])
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            self._channels[node_id] = channel
            self._stubs[node_id] = stub
        return stub

    def send(self, sender_id: str, recipient_id: str, message: Message) -> None:
        if isinstance(message, (RequestVoteReply, PreVoteReply, AppendEntriesReply)):
            return  # replies are returned synchronously by the gRPC server itself
        self._executor.submit(self._send_request_and_inject_reply, recipient_id, message)

    def _send_request_and_inject_reply(self, recipient_id: str, message: Message) -> None:
        try:
            stub = self._stub_for(recipient_id)
            if isinstance(message, RequestVoteArgs):
                proto_reply = stub.RequestVote(
                    serialization.request_vote_args_to_proto(message), timeout=self._call_timeout
                )
                reply = serialization.request_vote_reply_from_proto(proto_reply)
            elif isinstance(message, PreVoteArgs):
                proto_reply = stub.PreVote(
                    serialization.prevote_args_to_proto(message), timeout=self._call_timeout
                )
                reply = serialization.prevote_reply_from_proto(proto_reply)
            elif isinstance(message, AppendEntriesArgs):
                proto_reply = stub.AppendEntries(
                    serialization.append_entries_args_to_proto(message), timeout=self._call_timeout
                )
                reply = serialization.append_entries_reply_from_proto(proto_reply)
            else:
                raise TypeError(f"unexpected outbound message type: {type(message)!r}")
        except grpc.RpcError as exc:
            # Dropped, exactly like a lost packet on a real network -- Raft
            # already tolerates this (a missing reply just means no vote/ack
            # counted this round); nothing further to do.
            logger.debug("RPC to %s failed: %s", recipient_id, exc)
            return

        if self._runtime is not None:
            self._runtime.receive(
                Envelope(sender_id=recipient_id, recipient_id=self._self_node_id, message=reply)
            )
