# DARS

This is a _DAP2_ server written in Rust aimed at being fast and lightweight. It supports a subset of the [OPeNDAP protocol](https://opendap.github.io/documentation/UserGuideComprehensive.html). It is developed for use through the [NetCDF library](https://www.unidata.ucar.edu/software/netcdf/) (e.g. ncdump) or the [python NetCDF-bindings](https://unidata.github.io/netcdf4-python/netCDF4/index.html).

# Features
* Asynchronous
* Fast

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

* NetCDF and [NcML](https://www.unidata.ucar.edu/software/netcdf-java/current/ncml/Aggregation.html) (only aggregation along existing dimension). Groups are not supported, only variables and gridded variables on root are supported.

Some parts of OPenDAP or services commonly available with other implementation are not indented to be supporetd:

* Constraints except hyperslabs.
* catalog.xml.
* HTML interface.

For some of those _dars_ can be plugged in as a DAP-interface, while the rest is served using another implementation (like [Thredds](https://www.unidata.ucar.edu/software/tds/current/)).

