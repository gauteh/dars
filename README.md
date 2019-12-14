# DARS

This is a DAP2 server written in Rust aimed at being fast and lightweight. It supports a subset of the [OPeNDAP protocol](https://opendap.github.io/documentation/UserGuideComprehensive.html). It is developed for use through the [NetCDF library](https://www.unidata.ucar.edu/software/netcdf/) (e.g. ncdump) or the [python NetCDF-bindings](https://unidata.github.io/netcdf4-python/netCDF4/index.html).

# Features
* Asynchronous
* Low memory footprint
* Fast

# Running

Install [rustup nightly](https://github.com/rust-lang/rustup#working-with-nightly-rust), then do:

```sh
$ RUST_LOG=info cargo run --release
```

or build the Docker image:

```sh
$ docker build -t dars .
$ docker run -it -p 8001:8001 dars
```

NetCDF and NcML files in `data/` are served.

## Supported parts of OPeNDAP
* Hyperslabs and variable selection [constraints](https://opendap.github.io/documentation/UserGuideComprehensive.html#Constraint_Expressions).
* Variables on grids, but not deeper nested groups.

## Supported file formats
* NetCDF
* [NcML](https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html) (only aggregation along existing dimension).

## Parts of OPeNDAP unlikely to be supported
* Constraints except hyperslabs.
* catalog.xml.
* HTML interface.
