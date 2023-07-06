cmake_minimum_required(VERSION 3.21)

project(arra_tests VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

function(configure_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC} "${CMAKE_SOURCE_DIR}/tests/arrow/acero/test_util_internal.cc")
  target_link_libraries(${TEST_NAME} arra Arrow::arrow_testing_shared GTest::GTest GTest::Main pthread)
  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  # With OSX and conda, we need to set the correct RPATH so that dependencies
  # are found. The installed libraries with conda have an RPATH that matches
  # for executables and libraries lying in $ENV{CONDA_PREFIX}/bin or
  # $ENV{CONDA_PREFIX}/lib but our test libraries and executables are not
  # installed there.
  if(ARRA_USE_INTERNAL_ARROW AND NOT "$ENV{CONDA_PREFIX}" STREQUAL "" AND APPLE)
    set_target_properties(${TEST_NAME}
      PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
      INSTALL_RPATH_USE_LINK_PATH TRUE
      INSTALL_RPATH "${EXECUTABLE_OUTPUT_PATH};${ARROW_PREFIX}/lib;$ENV{CONDA_PREFIX}/lib")
  endif()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction(configure_test)

include_directories("${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")

set(FOO_TEST_SRC
  "${CMAKE_CURRENT_SOURCE_DIR}/foo_test.cpp")
configure_test(foo_test "${FOO_TEST_SRC}")

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/sketch")