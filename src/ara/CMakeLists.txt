cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_LIBRARIES)

set(ARA_DEPENDENCIES Arrow::arrow_shared ArrowCompute::arrow_compute_shared ArrowAcero::arrow_acero_shared Folly::folly)

if(ARA_CHECK_INTERNAL_DEPENDENCIES)
  add_custom_target(check-internal-dependencies)
endif(ARA_CHECK_INTERNAL_DEPENDENCIES)

macro(add_ara_library ARA_LIB_NAME ARA_LIB_SRC)
  add_library(${ARA_LIB_NAME} OBJECT ${ARA_LIB_SRC})
  target_include_directories(${ARA_LIB_NAME} PRIVATE "${CMAKE_SOURCE_DIR}/src")
  target_link_libraries(${ARA_LIB_NAME} PRIVATE ${ARA_DEPENDENCIES})

  list(APPEND ARA_LIBRARIES $<TARGET_OBJECTS:${ARA_LIB_NAME}>)
  set(ARA_LIBRARIES ${ARA_LIBRARIES} PARENT_SCOPE)

  if(ARA_CHECK_INTERNAL_DEPENDENCIES)
    add_library(${ARA_LIB_NAME}-dependency-check SHARED $<TARGET_OBJECTS:${ARA_LIB_NAME}>)
    target_include_directories(${ARA_LIB_NAME}-dependency-check PUBLIC "${CMAKE_SOURCE_DIR}/src")
    target_link_libraries(${ARA_LIB_NAME}-dependency-check PUBLIC ${ARA_DEPENDENCIES})

    add_dependencies(check-internal-dependencies ${ARA_LIB_NAME}-dependency-check)
  endif(ARA_CHECK_INTERNAL_DEPENDENCIES)
endmacro(add_ara_library)

macro(add_ara_internal_dependencies ARA_LIB_NAME DEPS)
  if(ARA_CHECK_INTERNAL_DEPENDENCIES)
    foreach(DEP ${DEPS})
      target_link_libraries(${ARA_LIB_NAME}-dependency-check PRIVATE $<TARGET_OBJECTS:${DEP}>)
    endforeach()
  endif(ARA_CHECK_INTERNAL_DEPENDENCIES)
endmacro(add_ara_internal_dependencies)

add_subdirectory(common)
add_subdirectory(driver)
add_subdirectory(executor)
add_subdirectory(pipeline)
add_subdirectory(plan)
add_subdirectory(schedule)
add_subdirectory(task)
add_subdirectory(util)

add_library(ara SHARED ${ARA_LIBRARIES})
target_include_directories(ara PRIVATE "${RAPIDJSON_INCLUDE_DIRS}")
target_include_directories(ara PUBLIC "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(ara PUBLIC ${ARA_DEPENDENCIES})
