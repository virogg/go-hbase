# go-hbase

HBase coprocessor streaming for Go: write HBase Observer-coprocessors in Go,
running as a long-lived process that talks to the Java RegionServer via
shared-memory ring buffers (no fork-per-call as in Hadoop Streaming).

> Status: bootstrap. See `SPEC.md` for the full spec and `tasks/plan.md` for the
> roadmap.

## Quick links

- Spec: [`SPEC.md`](SPEC.md)
- Implementation plan: [`tasks/plan.md`](tasks/plan.md)
- Todo: [`tasks/todo.md`](tasks/todo.md)
- IPC primitive: [`virogg/java-go-shmem`](https://github.com/virogg/java-go-shmem)

## License

Apache 2.0. See [`LICENSE`](LICENSE).
