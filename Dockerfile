FROM rustlang/rust:nightly

RUN apt-get -y update
RUN apt-get -y install cmake

ADD data /data

WORKDIR /work
ADD . .

# E.g. RUSTFLAGS=--target-cpu=native for optimizing build for host CPU.
ARG RUSTFLAGS
ENV RUSTFLAGS=${RUSTFLAGS:-}

RUN cargo install --path dars

EXPOSE 8001

ENV RUST_LOG=info

ENTRYPOINT [ "dars" ]
CMD [ "-a", "0.0.0.0:80", "/data/" ]

