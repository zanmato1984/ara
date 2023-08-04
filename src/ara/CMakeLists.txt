cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_DEPENDENCIES Arrow::arrow_shared ArrowAcero::arrow_acero_shared Folly::folly)

function(add_ara_library ARA_LIB_NAME ARA_LIB_SRC)
  add_library(${ARA_LIB_NAME} OBJECT ${ARA_LIB_SRC})
  target_include_directories(${ARA_LIB_NAME} PRIVATE "${CMAKE_SOURCE_DIR}/src")
  target_link_libraries(${ARA_LIB_NAME} PRIVATE ${ARA_DEPENDENCIES})

  if(ARA_CHECK_INTERNAL_DEPENDENCIES)
    add_library(${ARA_LIB_NAME}-dependency-check SHARED $<TARGET_OBJECTS:${ARA_LIB_NAME}>)
    target_include_directories(${ARA_LIB_NAME}-dependency-check PRIVATE "${CMAKE_SOURCE_DIR}/src")
  endif(ARA_CHECK_INTERNAL_DEPENDENCIES)
endfunction(add_ara_library)

function(add_ara_internal_dependencies ARA_LIB_NAME DEPS)
  if(ARA_CHECK_INTERNAL_DEPENDENCIES)
    foreach(DEP ${DEPS})
      target_link_libraries(${ARA_LIB_NAME}-dependency-check PRIVATE $<TARGET_OBJECTS:${DEP}>)
    endforeach()
  endif(ARA_CHECK_INTERNAL_DEPENDENCIES)
endfunction(add_ara_internal_dependencies)

add_subdirectory(common)
add_subdirectory(operator)
add_subdirectory(task)
add_subdirectory(util)

add_library(ara SHARED $<TARGET_OBJECTS:ara-common> $<TARGET_OBJECTS:ara-operator>)
target_include_directories(ara PRIVATE "${RAPIDJSON_INCLUDE_DIRS}")
target_include_directories(ara PUBLIC "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(ara PUBLIC ${ARA_DEPENDENCIES})
