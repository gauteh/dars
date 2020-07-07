# DARS

This is an *asynchronous* _DAP/2_ server written in Rust aimed at being *fast* and *lightweight*. It supports a subset of the [OPeNDAP protocol](https://opendap.github.io/documentation/UserGuideComprehensive.html). It is intended to only serve the DAP protocol, and not implement other common services like a catalog or a WMS.

# Running

Install [rustup nightly](https://github.com/rust-lang/rustup#working-with-nightly-rust), then do:

```sh
$ cargo run --release
```

or build the Docker image:

```sh
$ docker build -t dars .
$ docker run -it -p 8001:80 dars
```

> mount your data at `/data`.

NetCDF and NcML files in `data/` are served.

## OPeNDAP specification

_dars_ currently supports hyperslabs (except strides) and variable selection [constraints](https://opendap.github.io/documentation/UserGuideComprehensive.html#Constraint_Expressions).

File formats:

* HDF5 or HDF5 backed NetCDF (version 4)
* [NcML](https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html) (only aggregation along existing dimension).

> Dataset groups are not supported, only variables and gridded variables on root.

