#!/bin/bash

curl -X 'POST' \
    'http://localhost:8000/ingress/fhir' \
    -H 'accept: application/json' \
    -H 'Content-Type: multipart/form-data' \
    -F 'file=@Joe656_Lynch190_955b1b61-e049-4048-a42a-a45202060cf9.json'

# curl -X POST \
#     'http://localhost:8000/ingress/fhir/stream' \
#     -H 'accept: application/json' \
#     -H 'Content-Type: application/octet-stream' \
#     -T Joe656_Lynch190_955b1b61-e049-4048-a42a-a45202060cf9.json