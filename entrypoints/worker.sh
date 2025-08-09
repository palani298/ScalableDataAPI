#!/usr/bin/env bash
set -euo pipefail

python -m grpc_tools.protoc -I app/dataservice/proto \
  --python_out=app/dataservice/gen \
  --grpc_python_out=app/dataservice/gen \
  app/dataservice/proto/blog.proto || true

exec python -m app.worker.worker 