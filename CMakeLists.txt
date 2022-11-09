cmake_minimum_required(VERSION 3.21)

project(ARRA VERSION 0.0.1 LANGUAGES C CXX)

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
    -DARROW_EXTRA_ERROR_CONTEXT=ON
    "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>")
  ExternalProject_Add(arrow_ep
    PREFIX ${ARROW_PREFIX}
    SOURCE_SUBDIR cpp
    URL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-10.0.0.tar.gz"
    URL_HASH "SHA256=2852b21f93ee84185a9d838809c9a9c41bf6deca741bed1744e0fdba6cc19e3f"
    BUILD_IN_SOURCE 1
    CMAKE_ARGS ${ARROW_CMAKE_ARGS})
  file(MAKE_DIRECTORY "${ARROW_PREFIX}/include")
  add_library(Arrow::arrow_shared SHARED IMPORTED)
  set_target_properties(Arrow::arrow_shared PROPERTIES IMPORTED_LOCATION arrow INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(Arrow::arrow_shared arrow_ep)
else(ARRA_USE_INTERNAL_ARROW)
  message(STATUS "Using system Arrow library")
  include(cmake/find/arrow.cmake)
endif(ARRA_USE_INTERNAL_ARROW)

include(cmake/find/rapidjson.cmake)
include(cmake/find/gtest.cmake)

add_library(arra
  "${CMAKE_SOURCE_DIR}/src/main.cpp")

include_directories("${CMAKE_SOURCE_DIR}/include" "${RAPIDJSON_INCLUDE_DIRS}")
target_link_libraries(arra PRIVATE Arrow::arrow_shared)

if(ARRA_BUILD_TESTS)
  message(STATUS "Building the Arra googletest unit tests")
  add_subdirectory(${CMAKE_SOURCE_DIR}/tests)
  message(STATUS "ARRA_TEST_LIST set to: ${ARA_TEST_LIST}")
endif(ARRA_BUILD_TESTS)
