# NSQ Client Library in OCaml

A simple client library for the [NSQ](http://nsq.io) message platform, built on [Eio](https://github.com/ocaml-multicore/eio).

See `examples/` for a simple program that publishes and subscribes on the same topic.

## Getting started

Install dependencies and build:

```
opam install . --deps-only --with-test
make
```

Spin up NSQ using docker compose:

```
docker compose up -d
```

Run the example:

```
_build/default/examples/example.exe
```
