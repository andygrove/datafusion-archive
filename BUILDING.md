
# Building DataFusion

## Prerequisites

- Follow instructions at https://github.com/sfackler/rust-openssl to install OpenSSL
- If you want to build the docker image then it is necessary to install OpenSSL from source

## Building Locally

```bash
$ cargo build --release
```

## Building Docker Image

```bash
$ cd openssl
$ ./Configure --prefix=/usr linux-x86_64 -fPIC
```

```bash
$ cargo build --target=x86_64-unknown-linux-musl --release
```