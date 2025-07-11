FROM ubuntu:24.04 AS builder

WORKDIR /VK_Sirius_Database

COPY ./include ./include
COPY ./src ./src
COPY ./CMakeLists2.txt ./CMakeLists.txt

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    cmake \
    build-essential \
    git \
    clang-16 \
    lld-16 \
    libc++-16-dev \
    libc++abi-16-dev \
    libssl-dev \
    ca-certificates && \
    update-ca-certificates --fresh && \
    rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/clang-16 \
    CXX=/usr/bin/clang++-16 \
    LD=/usr/bin/ld.lld-16

RUN mkdir cmake-build && cd cmake-build && \
    cmake -DBUILD_WEB_SERVER=ON \
    -DCMAKE_CXX_STANDARD=20 \
    -DCMAKE_BUILD_TYPE=Release .. && \
    make -j$(nproc)

FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libc++1-16 \
    libc++abi1-16 \
    libssl3 \
    ca-certificates \
    libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /VK_Sirius_Database/cmake-build/database_server /app/

RUN mkdir -p /app/logs

EXPOSE 8080

CMD ["./database_server"]
