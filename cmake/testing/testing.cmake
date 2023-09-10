if(ARA_BUILD_TESTS)
  if(NOT ARA_USE_INTERNAL_ARROW)
    message(FATAL_ERROR "Can't build Ara tests without internal Arrow library")
  endif(NOT ARA_USE_INTERNAL_ARROW)
  message(STATUS "Building the Ara googletest unit tests")
  include(cmake/find/gtest.cmake)
  include(cmake/find/gflags.cmake)
endif(ARA_BUILD_TESTS)

set(EXTRA_ARROW_TEST_SRC "${CMAKE_SOURCE_DIR}/src/arrow/acero/test_util_internal.cc")

function(add_ara_test TEST_NAME ARA_LIB_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC} ${EXTRA_ARROW_TEST_SRC})
  if(ARA_CHECK_INTERNAL_DEPENDENCIES)
    set(ARA_LIB_NAME ${ARA_LIB_NAME}-dependency-check)
    set(ARA_LIB_DIR $<TARGET_FILE_DIR:${ARA_LIB_NAME}>)
  else()
    set(ARA_LIB_NAME "ara")
    set(ARA_LIB_DIR "${CMAKE_BINARY_DIR}/src/ara")
  endif(ARA_CHECK_INTERNAL_DEPENDENCIES)
  target_link_libraries(${TEST_NAME} ${ARA_LIB_NAME} Arrow::arrow_testing_shared GTest::GTest GTest::Main pthread)
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
      INSTALL_RPATH "${EXECUTABLE_OUTPUT_PATH};${ARA_LIB_DIR};${ARROW_PREFIX}/lib;$ENV{CONDA_PREFIX}/lib")
  endif()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction(add_ara_test)

function(add_sketch_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC} ${EXTRA_ARROW_TEST_SRC})
  target_include_directories(${TEST_NAME} PRIVATE "${CMAKE_SOURCE_DIR}/src")
  target_link_libraries(${TEST_NAME} PUBLIC Arrow::arrow_shared ArrowAcero::arrow_acero_shared Folly::folly Arrow::arrow_testing_shared GTest::GTest GTest::Main pthread)
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
      INSTALL_RPATH "${EXECUTABLE_OUTPUT_PATH};${ARROW_PREFIX}/lib;$ENV{CONDA_PREFIX}/lib")
  endif()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction(add_sketch_test)
