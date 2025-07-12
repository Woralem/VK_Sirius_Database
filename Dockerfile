FROM gcc:latest AS builder

WORKDIR /VK_Sirius_Database

COPY ./include ./include
COPY ./src ./src
COPY ./CMakeLists.txt ./CMakeLists.txt

# Устанавливаем зависимости для сборки
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    cmake \
    make \
    git \
    libc6 \
    libcurl4-openssl-dev \
    libssl-dev \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Собираем и устанавливаем CPR
RUN git clone https://github.com/libcpr/cpr.git && \
    cd cpr && \
    mkdir build && \
    cd build && \
    cmake .. \
        -DCPR_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCPR_FORCE_USE_SYSTEM_CURL=ON \
        -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && \
    make install && \
    ldd /usr/local/lib/libcpr.so.1

# Собираем основное приложение
RUN mkdir cmake-build && cd cmake-build && \
    cmake -DBUILD_WEB_SERVER=ON \
          -DCMAKE_CXX_STANDARD=20 \
          -DCMAKE_BUILD_TYPE=Release .. && \
    make -j$(nproc)

# Находим все необходимые библиотеки
RUN mkdir -p /tmp/libs && \
    ldd /VK_Sirius_Database/cmake-build/VK_Database_API_Layer | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' /tmp/libs/ && \
    cp /lib64/ld-linux-x86-64.so.2 /tmp/libs/

FROM ubuntu:latest

# Устанавливаем runtime-зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libcurl4 \
    libc6 \
    libssl3 \
    libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем приложение и все необходимые библиотеки
COPY --from=builder /VK_Sirius_Database/cmake-build/VK_Database_API_Layer /app/
COPY --from=builder /tmp/libs/* /usr/lib/x86_64-linux-gnu/

EXPOSE 8090
CMD ["./VK_Database_API_Layer"]