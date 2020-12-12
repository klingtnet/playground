#!/usr/bin/env bash

curl 'https://ziglang.org/download/0.7.0/zig-linux-x86_64-0.7.0.tar.xz' | tar -xJvf-
curl -Lsf 'https://github.com/zigtools/zls/releases/download/0.1.0/x86_64-linux.tar.xz' | tar --strip-components=1 -C zig-linux-x86_64-0.7.0/ -xJvf- x86_64-linux/zls
