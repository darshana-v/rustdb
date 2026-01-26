# RustDB

A project to understand database internals, rust and an attempt at building something real from scratch.

## Scope

- single-node
- client/server (TCP)
- SQL-like interface (also in rust: [rsh](https://github.com/darshana-v/rsh/))
- understand page layout, buffer pools, B-trees, write-ahead logging, recovery, concurrency
- basic structure enough to accommodate future replication, sharding, and optimization without rewrites

## Architecture

```
Client (rsh) -> TCP Protocol -> Query Layer (Parser -> Planner -> Executor)

Transaction Manager -> Storage Engine (Heap + B-tree) -> Buffer -> Disk + WAL
```
