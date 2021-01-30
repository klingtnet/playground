#!/usr/bin/env bash

curl -Lsf 'https://github.com/letsencrypt/pebble/releases/download/v2.3.1/pebble_linux-amd64' > pebble
curl -Lsf 'https://raw.githubusercontent.com/letsencrypt/pebble/v2.3.1/test/certs/localhost/cert.pem' > pebble-cert.pem
curl -Lsf 'https://raw.githubusercontent.com/letsencrypt/pebble/v2.3.1/test/certs/localhost/key.pem' > pebble-key.pem
chmod +x pebble
