#!/usr/bin/env bash
# Regenerates coordinator/grpc/raft_pb2*.py from proto/raft.proto.
#
# protoc's generated raft_pb2_grpc.py uses a plain `import raft_pb2`, which
# breaks once this package is imported as coordinator.grpc.raft_pb2_grpc
# from elsewhere -- the sed below rewrites it to a package-relative import.
# Re-run this (and reapply the fix) any time raft.proto changes.
set -euo pipefail
cd "$(dirname "$0")/.."

python -m grpc_tools.protoc \
  -I proto \
  --python_out=coordinator/grpc \
  --grpc_python_out=coordinator/grpc \
  proto/raft.proto

sed -i '' 's/^import raft_pb2 as raft__pb2$/from . import raft_pb2 as raft__pb2/' coordinator/grpc/raft_pb2_grpc.py

echo "Regenerated coordinator/grpc/raft_pb2.py and raft_pb2_grpc.py"
