"""Conversions between raft/types.py dataclasses and the generated protobuf
messages (proto/raft.proto). `command` payloads are opaque to Raft itself,
so they're pickled rather than given their own proto schema -- every node
in the cluster runs the same trusted Python code, so this isn't meant to
support cross-language peers or untrusted input.
"""

from __future__ import annotations

import pickle

from coordinator.grpc import raft_pb2
from raft.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    PreVoteArgs,
    PreVoteReply,
    RequestVoteArgs,
    RequestVoteReply,
)


def log_entry_to_proto(entry: LogEntry) -> raft_pb2.LogEntry:
    return raft_pb2.LogEntry(term=entry.term, index=entry.index, command=pickle.dumps(entry.command))


def log_entry_from_proto(proto: raft_pb2.LogEntry) -> LogEntry:
    return LogEntry(term=proto.term, index=proto.index, command=pickle.loads(proto.command))


def request_vote_args_to_proto(args: RequestVoteArgs) -> raft_pb2.RequestVoteArgs:
    return raft_pb2.RequestVoteArgs(
        term=args.term,
        candidate_id=args.candidate_id,
        last_log_index=args.last_log_index,
        last_log_term=args.last_log_term,
    )


def request_vote_args_from_proto(proto: raft_pb2.RequestVoteArgs) -> RequestVoteArgs:
    return RequestVoteArgs(
        term=proto.term,
        candidate_id=proto.candidate_id,
        last_log_index=proto.last_log_index,
        last_log_term=proto.last_log_term,
    )


def request_vote_reply_to_proto(reply: RequestVoteReply) -> raft_pb2.RequestVoteReply:
    return raft_pb2.RequestVoteReply(
        term=reply.term, vote_granted=reply.vote_granted, voter_id=reply.voter_id
    )


def request_vote_reply_from_proto(proto: raft_pb2.RequestVoteReply) -> RequestVoteReply:
    return RequestVoteReply(term=proto.term, vote_granted=proto.vote_granted, voter_id=proto.voter_id)


def prevote_args_to_proto(args: PreVoteArgs) -> raft_pb2.PreVoteArgs:
    return raft_pb2.PreVoteArgs(
        term=args.term,
        candidate_id=args.candidate_id,
        last_log_index=args.last_log_index,
        last_log_term=args.last_log_term,
    )


def prevote_args_from_proto(proto: raft_pb2.PreVoteArgs) -> PreVoteArgs:
    return PreVoteArgs(
        term=proto.term,
        candidate_id=proto.candidate_id,
        last_log_index=proto.last_log_index,
        last_log_term=proto.last_log_term,
    )


def prevote_reply_to_proto(reply: PreVoteReply) -> raft_pb2.PreVoteReply:
    return raft_pb2.PreVoteReply(term=reply.term, vote_granted=reply.vote_granted, voter_id=reply.voter_id)


def prevote_reply_from_proto(proto: raft_pb2.PreVoteReply) -> PreVoteReply:
    return PreVoteReply(term=proto.term, vote_granted=proto.vote_granted, voter_id=proto.voter_id)


def append_entries_args_to_proto(args: AppendEntriesArgs) -> raft_pb2.AppendEntriesArgs:
    return raft_pb2.AppendEntriesArgs(
        term=args.term,
        leader_id=args.leader_id,
        prev_log_index=args.prev_log_index,
        prev_log_term=args.prev_log_term,
        entries=[log_entry_to_proto(e) for e in args.entries],
        leader_commit=args.leader_commit,
    )


def append_entries_args_from_proto(proto: raft_pb2.AppendEntriesArgs) -> AppendEntriesArgs:
    return AppendEntriesArgs(
        term=proto.term,
        leader_id=proto.leader_id,
        prev_log_index=proto.prev_log_index,
        prev_log_term=proto.prev_log_term,
        entries=tuple(log_entry_from_proto(e) for e in proto.entries),
        leader_commit=proto.leader_commit,
    )


def append_entries_reply_to_proto(reply: AppendEntriesReply) -> raft_pb2.AppendEntriesReply:
    return raft_pb2.AppendEntriesReply(
        term=reply.term,
        success=reply.success,
        responder_id=reply.responder_id,
        match_index=reply.match_index,
        conflict_index=reply.conflict_index,
        conflict_term=reply.conflict_term,
    )


def append_entries_reply_from_proto(proto: raft_pb2.AppendEntriesReply) -> AppendEntriesReply:
    return AppendEntriesReply(
        term=proto.term,
        success=proto.success,
        responder_id=proto.responder_id,
        match_index=proto.match_index,
        conflict_index=proto.conflict_index,
        conflict_term=proto.conflict_term,
    )
