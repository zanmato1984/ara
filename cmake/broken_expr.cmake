# Local source override for development; the released pin is a reviewed core SHA.
set(ARA_BROKEN_EXPR_SOURCE_DIR "" CACHE PATH "broken-expr-rs source checkout")
option(ARA_BROKEN_EXPR_DISCONNECTED_TEST "Build-only negative control, never production" OFF)
if(NOT ARA_BROKEN_EXPR_SOURCE_DIR)
  include(FetchContent)
  FetchContent_Declare(broken_expr_source
    GIT_REPOSITORY https://github.com/zanmato1984/broken-expr-rs.git
    GIT_TAG fcfb45241b22c831b5c49c7e0f0769bd484c6890)
  FetchContent_MakeAvailable(broken_expr_source)
  set(ARA_BROKEN_EXPR_SOURCE_DIR "${broken_expr_source_SOURCE_DIR}")
endif()
find_program(CARGO_EXECUTABLE cargo REQUIRED)
set(BROKEN_EXPR_TARGET_DIR "${CMAKE_BINARY_DIR}/broken-expr-target")
set(BROKEN_EXPR_ARCHIVE "${BROKEN_EXPR_TARGET_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}broken_expr_c${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(BROKEN_EXPR_FEATURES)
if(ARA_BROKEN_EXPR_DISCONNECTED_TEST)
  list(APPEND BROKEN_EXPR_FEATURES --features disconnected-test)
endif()
file(GLOB_RECURSE BROKEN_EXPR_SOURCES CONFIGURE_DEPENDS
  "${ARA_BROKEN_EXPR_SOURCE_DIR}/crates/broken-expr/src/*.rs"
  "${ARA_BROKEN_EXPR_SOURCE_DIR}/crates/broken-expr-c/src/*.rs"
  "${ARA_BROKEN_EXPR_SOURCE_DIR}/crates/*/Cargo.toml")
add_custom_command(OUTPUT "${BROKEN_EXPR_ARCHIVE}"
  COMMAND "${CARGO_EXECUTABLE}" build --locked --release -p broken-expr-c
    --manifest-path "${ARA_BROKEN_EXPR_SOURCE_DIR}/Cargo.toml"
    --target-dir "${BROKEN_EXPR_TARGET_DIR}" ${BROKEN_EXPR_FEATURES}
  DEPENDS ${BROKEN_EXPR_SOURCES} "${ARA_BROKEN_EXPR_SOURCE_DIR}/Cargo.lock"
  VERBATIM)
add_custom_target(broken-expr-build DEPENDS "${BROKEN_EXPR_ARCHIVE}")
add_library(broken-expr STATIC IMPORTED GLOBAL)
set_target_properties(broken-expr PROPERTIES IMPORTED_LOCATION "${BROKEN_EXPR_ARCHIVE}"
  INTERFACE_INCLUDE_DIRECTORIES "${ARA_BROKEN_EXPR_SOURCE_DIR}/crates/broken-expr-c/include")
add_dependencies(broken-expr broken-expr-build)
