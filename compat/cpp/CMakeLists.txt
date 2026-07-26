cmake_minimum_required(VERSION 3.20)
project(pg_kinetic_compat_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)

add_executable(compat_libpqxx main.cpp)
target_link_libraries(compat_libpqxx PRIVATE PkgConfig::PQXX)
