cmake_minimum_required(VERSION 3.20)
project(pg_kinetic_compat_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(libpqxx CONFIG REQUIRED)

add_executable(compat_libpqxx main.cpp)
target_link_libraries(compat_libpqxx PRIVATE libpqxx::pqxx)
