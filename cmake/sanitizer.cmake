if(ARA_USE_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -DADDRESS_SANITIZER")
  message(STATUS "Using Address Sanitizer")
endif(ARA_USE_ASAN)
