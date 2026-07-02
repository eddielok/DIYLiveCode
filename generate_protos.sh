#!/bin/bash
# Generate Python gRPC files from proto definitions

python3 -m grpc_tools.protoc \
  -I./proto \
  --python_out=. \
  --grpc_python_out=. \
  ./proto/capture.proto

echo "✓ Proto files generated: proto/capture_pb2.py, proto/capture_pb2_grpc.py"
