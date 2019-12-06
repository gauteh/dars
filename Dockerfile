FROM rustlang/rust:nightly

RUN apt-get -y update
RUN apt-get -y install libnetcdf-dev

WORKDIR /work
ADD . .

RUN cargo install --path .

CMD ["dars"]

