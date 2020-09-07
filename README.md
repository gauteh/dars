[![Build Status](https://travis-ci.org/gauteh/dars.svg?branch=master)](https://travis-ci.org/gauteh/dars)
[![Docker Automated build](https://img.shields.io/docker/cloud/automated/gauteh/dars)](https://hub.docker.com/r/gauteh/dars)

# 𓃢   DARS

DARS is an *asynchronous* _DAP/2_ server written in Rust aimed at being *fast* and *lightweight*. It supports a subset of the [OPeNDAP protocol](https://opendap.github.io/documentation/UserGuideComprehensive.html). It aims to only serve the `DAP` protocol, not common services like a catalog or a WMS.

## OPeNDAP server implementation and file formats

Variable and hyperslab [constraints](https://opendap.github.io/documentation/UserGuideComprehensive.html#Constraint_Expressions), _except strides_, are implemented. File formats based on `HDF5` are supported:

* [HDF5](https://www.hdfgroup.org/solutions/hdf5/)
* [NetCDF](https://www.unidata.ucar.edu/software/netcdf/) (version 4)
* [NcML](https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html) (aggregation along existing dimension).

HDF5 is read through [hidefix](https://github.com/gauteh/hidefix), which is an
experimental HDF5 reader for concurrent reading.

# Installation and basic usage

Set up [rustup nightly](https://github.com/rust-lang/rustup#working-with-nightly-rust).

Running from the repository:

```sh
$ cargo run --release
```

or install with:

```sh
$ cargo install --path dars
```

A list of datasets and DAP URLs can be queried at: `http://localhost:8001/data/` (use `curl -Haccept:application/json http://localhost:8001/data/` to JSON). Use e.g. `ncdump -h http://..` to explore the datasets.

## Docker

Use [gauteh/dars](https://hub.docker.com/repository/docker/gauteh/dars) or build yourself:

```sh
$ docker build -t dars .
$ docker run -it -p 8001:80 dars
```

mount your data at `/data`.

