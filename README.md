# dumpstors ![CI](https://github.com/romhml/dumpstors/workflows/Tests/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/romhml/dumpstors/badge.svg?branch=main)](https://coveralls.io/github/romhml/dumpstors?branch=main) ![Security audit](https://github.com/romhml/dumpstors/workflows/Security%20audit/badge.svg)
## Work in progress
A key-value store implemented in rust, accessible through a gRPC API.

## Getting started
### Using docker
Start the server:
```bash
$ docker run -p 4242 -it romhml/dumpstors:latest
```

## Command Line Interface
### Using docker
```bash
$ docker run -it romhml/dumpstors:latest -- /bin/bash
$ dumpstors help
```
