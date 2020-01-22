FROM rustlang/rust:nightly

RUN apt-get -y update
RUN apt-get -y install libnetcdf-dev

ADD data /data

WORKDIR /work
ADD . .

RUN cargo install --path .

EXPOSE 80

ENV RUST_LOG=info

ENTRYPOINT [ "dars" ]
CMD [ "-a", "0.0.0.0:80", "/data/" ]

