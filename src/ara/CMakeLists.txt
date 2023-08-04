cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

add_library(ara INTERFACE)
set(ARA_DEPENDENCIES Arrow::arrow_shared ArrowAcero::arrow_acero_shared Folly::folly)
target_include_directories(ara INTERFACE "${RAPIDJSON_INCLUDE_DIRS}")
target_include_directories(ara INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(ara INTERFACE ${ARA_DEPENDENCIES})

function(add_ara_library ARA_LIB_NAME ARA_LIB_SRC)
  add_library(${ARA_LIB_NAME} ${ARA_LIB_SRC})
#   target_include_directories(${ARA_LIB_NAME} PUBLIC "${DIR}")
  target_include_directories(${ARA_LIB_NAME} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
#   target_include_directories(${ARA_LIB_NAME} PUBLIC "${CMAKE_SOURCE_DIR}/src/ara")
  target_link_libraries(${ARA_LIB_NAME} PRIVATE ${ARA_DEPENDENCIES})
  target_link_libraries(ara INTERFACE ${ARA_LIB_NAME})
endfunction(add_ara_library)

add_subdirectory(common)
add_subdirectory(task)
add_subdirectory(operator)
