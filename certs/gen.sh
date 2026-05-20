#!/usr/bin/env bash
set -e
mkdir -p certs
cd certs
openssl ecparam -name prime256v1 -genkey -noout -out server.key
openssl req -new -x509 -key server.key -out server.crt \
    -days 3650 \
    -subj "/CN=quic-pubsub-dev" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
echo "Certificates generated: server.crt / server.key"
