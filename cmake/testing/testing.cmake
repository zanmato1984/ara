if(ARA_BUILD_TESTS)
  if(NOT ARA_USE_INTERNAL_ARROW)
    message(FATAL_ERROR "Can't build Ara tests without internal Arrow library")
  endif(NOT ARA_USE_INTERNAL_ARROW)
  message(STATUS "Building the Ara googletest unit tests")
  include(cmake/find/gtest.cmake)
  include(cmake/find/gflags.cmake)
endif(ARA_BUILD_TESTS)

set(EXTRA_ARROW_TEST_SRC "${CMAKE_SOURCE_DIR}/src/arrow/acero/test_util_internal.cc")

function(add_ara_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC} ${EXTRA_ARROW_TEST_SRC})
  target_link_libraries(${TEST_NAME} ara Arrow::arrow_testing_shared GTest::GTest GTest::Main pthread)
  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  # With OSX and conda, we need to set the correct RPATH so that dependencies
  # are found. The installed libraries with conda have an RPATH that matches
  # for executables and libraries lying in $ENV{CONDA_PREFIX}/bin or
  # $ENV{CONDA_PREFIX}/lib but our test libraries and executables are not
  # installed there.
  if(ARA_USE_INTERNAL_ARROW AND NOT "$ENV{CONDA_PREFIX}" STREQUAL "" AND APPLE)
    set_target_properties(${TEST_NAME}
      PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
      INSTALL_RPATH_USE_LINK_PATH TRUE
      INSTALL_RPATH "${EXECUTABLE_OUTPUT_PATH};${CMAKE_BINARY_DIR}/src/ara;${ARROW_PREFIX}/lib;$ENV{CONDA_PREFIX}/lib")
  endif()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction(add_ara_test)
