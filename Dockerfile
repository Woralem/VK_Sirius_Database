FROM gcc:14.2.0 AS builder
EXPOSE 8090
WORKDIR "/VK_Sirius_Database"
COPY "./include" "./include"
#df
COPY "./src" "./src"
COPY "./CMakeLists.txt" "./CMakeLists.txt"
#RUN apt-get update && apt-get install -y --fix-missing cmake
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    cmake \
    make \
    git \
    libcurl4-openssl-dev \
    libssl-dev \
    build-essential

RUN mkdir cmake-build && cd cmake-build && cmake .. && make
CMD ./cmake-build/VK_Database_API_Layer