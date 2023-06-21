# fluent-bit-plugin-ydb
YDB output plugin for FluentBit

Build args:

`-buildmode=c-shared -o binaries/ydb_plugin.so`

Usage:

`fluent-bit -i dummy -f 1 -e binaries/ydb_plugin.so -o ydb -p ConnectionURL="grpc://localhost:2136/local" -p Table="fbit"`