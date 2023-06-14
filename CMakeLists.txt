cmake_minimum_required(VERSION 3.21)

project(arra VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

option(ARRA_USE_INTERNAL_ARROW "Use internal Arrow library, usurally for debugging purpose" OFF)
option(ARRA_BUILD_TESTS "Build the Arra googletest unit tests" OFF)

if(ARRA_USE_INTERNAL_ARROW)
  message(STATUS "Using internal Arrow library")
  include(ExternalProject)
  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow")
  set(ARROW_CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DARROW_COMPUTE=ON
    -DARROW_CSV=ON
    -DARROW_DATASET=ON
    -DARROW_FILESYSTEM=ON
    -DARROW_JSON=ON
    -DARROW_PARQUET=ON
    -DARROW_SUBSTRAIT=ON
    -DARROW_ACERO=ON
    -DARROW_EXTRA_ERROR_CONTEXT=ON
    -DARROW_TESTING=ON
    "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>")
  set(ARROW_SHARED_LIBRARY "${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_ACERO_SHARED_LIBRARY "${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_acero${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_TESTING_SHARED_LIBRARY "${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_testing${CMAKE_SHARED_LIBRARY_SUFFIX}")
  ExternalProject_Add(arrow_ep
    PREFIX ${ARROW_PREFIX}
    SOURCE_SUBDIR cpp
    URL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-12.0.0.tar.gz"
    URL_HASH "SHA256=f25901c486e1e79cde8b78b3e7b1d889919f942549996003a7341a8ee86addaa"
    BUILD_IN_SOURCE 1
    CMAKE_ARGS "${ARROW_CMAKE_ARGS}")
  file(MAKE_DIRECTORY "${ARROW_PREFIX}/include")
  add_library(Arrow::arrow_shared SHARED IMPORTED)
  set_target_properties(Arrow::arrow_shared PROPERTIES IMPORTED_LOCATION "${ARROW_SHARED_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(Arrow::arrow_shared arrow_ep)
  add_library(ArrowAcero::arrow_acero_shared SHARED IMPORTED)
  set_target_properties(ArrowAcero::arrow_acero_shared PROPERTIES IMPORTED_LOCATION "${ARROW_ACERO_SHARED_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(ArrowAcero::arrow_acero_shared arrow_ep)
  add_library(Arrow::arrow_testing_shared SHARED IMPORTED)
  set_target_properties(Arrow::arrow_testing_shared PROPERTIES IMPORTED_LOCATION "${ARROW_TESTING_SHARED_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(Arrow::arrow_testing_shared arrow_ep)
else(ARRA_USE_INTERNAL_ARROW)
  message(STATUS "Using system Arrow library")
  include(cmake/find/arrow.cmake)
endif(ARRA_USE_INTERNAL_ARROW)

include(cmake/find/folly.cmake)
include(cmake/find/rapidjson.cmake)

add_library(arra
  "${CMAKE_SOURCE_DIR}/src/foo.cpp")

target_include_directories(arra PUBLIC "${CMAKE_SOURCE_DIR}/include" "${RAPIDJSON_INCLUDE_DIRS}")
target_link_libraries(arra PUBLIC Arrow::arrow_shared ArrowAcero::arrow_acero_shared Folly::folly)

if(ARRA_BUILD_TESTS)
  if(NOT ARRA_USE_INTERNAL_ARROW)
    message(FATAL_ERROR "Can't build Arra tests without internal Arrow library")
  endif(NOT ARRA_USE_INTERNAL_ARROW)
  message(STATUS "Building the Arra googletest unit tests")
  include(cmake/find/gtest.cmake)
  include(cmake/find/gflags.cmake)
  add_subdirectory("${CMAKE_SOURCE_DIR}/tests")
endif(ARRA_BUILD_TESTS)
