cmake_minimum_required(VERSION 3.21)

project(ara VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

option(ARA_USE_INTERNAL_ARROW "Use internal Arrow library, mostly for Ara developers" OFF)
option(ARA_BUILD_TESTS "Build the Ara googletest unit tests" OFF)
option(ARA_CHECK_INTERNAL_DEPENDENCIES "Add extra build targets to check dependencies between different Ara sub-libraries" OFF)
option(ARA_USE_ASAN "Use Address Sanitizer" OFF)

include(cmake/find/arrow.cmake)
include(cmake/find/folly.cmake)
include(cmake/find/rapidjson.cmake)
include(cmake/testing/testing.cmake)
include(cmake/sanitizer.cmake)

add_subdirectory("${CMAKE_SOURCE_DIR}/src/ara")
add_subdirectory("${CMAKE_SOURCE_DIR}/src/sketch")
