FROM rustlang/rust:nightly-buster-slim AS builder

RUN apt-get update && apt-get -y install cmake && rm -rf /var/lib/apt/lists/*

WORKDIR /work
ADD . .

# E.g. "RUSTFLAGS=-C target-cpu=native" for optimizing build for host CPU.
ARG RUSTFLAGS
RUN cargo install --path dars

FROM debian:stable-20220418-slim
WORKDIR /work/
COPY --from=builder /usr/local/cargo/bin/dars /usr/bin/dars
ADD ./entrypoint.sh .
ADD data /data

ENV DARS_PORT=${DARS_PORT:-8001}
EXPOSE ${DARS_PORT}
ENV RUST_LOG=${RUST_LOG:-info}

ENTRYPOINT [ "./entrypoint.sh" ]
CMD [ "/data/" ]
