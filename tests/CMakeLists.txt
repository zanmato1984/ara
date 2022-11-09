cmake_minimum_required(VERSION 3.21)

project(ARRA_TESTS VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

function(configure_test CMAKE_TEST_NAME CMAKE_TEST_SRC)
  add_executable(${CMAKE_TEST_NAME} ${CMAKE_TEST_SRC})
  target_link_libraries(${CMAKE_TEST_NAME} GTest::GTest GTest::Main pthread arra)
  set_target_properties(${CMAKE_TEST_NAME} PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${CMAKE_TEST_NAME} COMMAND ${CMAKE_TEST_NAME})
endfunction(configure_test)

include_directories("${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")

set(FOO_TEST_SRC
  "${CMAKE_CURRENT_SOURCE_DIR}/foo_test.cpp")
configure_test(FOO_TEST "${FOO_TEST_SRC}")
