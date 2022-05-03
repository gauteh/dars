FROM rustlang/rust:nightly-buster-slim AS builder

RUN apt-get update && apt-get -y install cmake && rm -rf /var/lib/apt/lists/*

WORKDIR /work
ADD . .

# E.g. "RUSTFLAGS=-C target-cpu=native" for optimizing build for host CPU.
ARG RUSTFLAGS
ENV RUSTFLAGS=${RUSTFLAGS:-}

RUN cargo install --path dars

FROM debian:stable-20220418-slim
WORKDIR /work/
COPY --from=builder /usr/local/cargo/bin/dars /usr/bin/dars
ADD data /data

ARG DARS_PORT
ENV DARS_PORT=${DARS_PORT:-8001}
EXPOSE ${DARS_PORT}
ENV RUST_LOG=info

ENTRYPOINT [ "dars" ]
# TODO: MAke entrypoint shell script to expose DARS_PORT
CMD [ "-a", "0.0.0.0:8001", "/data/" ]
