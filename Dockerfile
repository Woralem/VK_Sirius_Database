FROM ubuntu:24.04  AS builder
EXPOSE 8080
WORKDIR "/VK_Sirius_Database"
COPY "./include" "./include"
COPY "./src" "./src"
COPY "./CmakeLists2.txt" "./CMakeLists.txt"
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
RUN mkdir cmake-build && cd cmake-build && cmake -DBUILD_WEB_SERVER=ON \
     -DCMAKE_CXX_STANDARD=20 \
    -DCMAKE_BUILD_TYPE=Release ..  \
    && make
CMD ./cmake-build/database_server
