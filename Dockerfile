FROM gcc:14.2.0 AS builder
EXPOSE 8080
WORKDIR "/VK_Sirius_Database"
COPY "./include" "./include"
COPY "./src" "./src"
COPY "./CMakeLists.txt" "./CMakeLists.txt"
RUN apt-get update && apt-get install -y --fix-missing cmake
RUN mkdir cmake-build && cd cmake-build && cmake .. && make
CMD ./cmake-build/database_server