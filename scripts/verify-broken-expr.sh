#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
: "${ARA_EXPR_BUILD_DIR:?Set ARA_EXPR_BUILD_DIR to an existing project-storage output directory}"
mkdir -p "$ARA_EXPR_BUILD_DIR"
df -h "$HOME"
args=(-G Ninja -DCMAKE_BUILD_TYPE=Release -DARA_USE_INTERNAL_ARROW=ON -DARA_BUILD_TESTS=ON
      -DARA_BROKEN_EXPR_DISCONNECTED_TEST=OFF)
args+=("-DARA_BROKEN_EXPR_SOURCE_DIR=${ARA_BROKEN_EXPR_SOURCE_DIR:-}")
if [[ $(uname -s) == Darwin ]]; then
  # Homebrew Folly exports its CLT SDK headers. Use the matching SDK/compiler.
  if [[ -d /Library/Developer/CommandLineTools ]]; then
    export DEVELOPER_DIR=${ARA_EXPR_DEVELOPER_DIR:-/Library/Developer/CommandLineTools}
  fi
  args+=("-DCMAKE_C_COMPILER=$(xcrun --find clang)" "-DCMAKE_CXX_COMPILER=$(xcrun --find clang++)"
         "-DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path)" "-DCMAKE_PREFIX_PATH=$(brew --prefix)")
fi
targets=(ara-expr-projection-test ara-hash-join-build-test ara-hash-join-probe-test ara-expression-inventory)
cmake -S "$root" -B "$ARA_EXPR_BUILD_DIR" "${args[@]}" > "$ARA_EXPR_BUILD_DIR/configure.log" 2>&1
cmake --build "$ARA_EXPR_BUILD_DIR" --target "${targets[@]}" -j "${ARA_EXPR_BUILD_JOBS:-4}" > "$ARA_EXPR_BUILD_DIR/build.log" 2>&1
run_connected() {
  for test in ara-expr-projection-test ara-hash-join-build-test ara-hash-join-probe-test; do
    "$ARA_EXPR_BUILD_DIR/gtests/$test" > "$ARA_EXPR_BUILD_DIR/$test.log" 2>&1
  done
}
restore_connected() {
  cmake -S "$root" -B "$ARA_EXPR_BUILD_DIR" -DARA_BROKEN_EXPR_DISCONNECTED_TEST=OFF > "$ARA_EXPR_BUILD_DIR/restore-configure.log" 2>&1
  cmake --build "$ARA_EXPR_BUILD_DIR" --target "${targets[@]}" -j "${ARA_EXPR_BUILD_JOBS:-4}" > "$ARA_EXPR_BUILD_DIR/restore-build.log" 2>&1
}
run_connected
"$ARA_EXPR_BUILD_DIR/ara-expression-inventory" > "$ARA_EXPR_BUILD_DIR/arrow-runtime.json"
# Same production test/library, with only the Rust kernel deliberately disabled.
# No second Arrow build or native duplicate implementation is introduced.
trap restore_connected EXIT
cmake -S "$root" -B "$ARA_EXPR_BUILD_DIR" -DARA_BROKEN_EXPR_DISCONNECTED_TEST=ON > "$ARA_EXPR_BUILD_DIR/negative-configure.log" 2>&1
cmake --build "$ARA_EXPR_BUILD_DIR" --target ara-expr-projection-test ara-hash-join-build-test -j "${ARA_EXPR_BUILD_JOBS:-4}" > "$ARA_EXPR_BUILD_DIR/negative-build.log" 2>&1
for test in ara-expr-projection-test ara-hash-join-build-test; do
  if "$ARA_EXPR_BUILD_DIR/gtests/$test" > "$ARA_EXPR_BUILD_DIR/negative-$test.log" 2>&1; then
    echo "negative control unexpectedly passed: $test" >&2
    exit 1
  fi
  if ! rg -q 'broken-expr projection failed, bridge code 2' "$ARA_EXPR_BUILD_DIR/negative-$test.log"; then
    echo "negative control did not reach the disconnected Rust kernel: $test" >&2
    exit 1
  fi
done
restore_connected
trap - EXIT
run_connected
df -h "$HOME"
echo "Production projection baseline comparison, 1 join-build + 18 join-probe tests, two disconnected negative controls, and restored connected tests passed."
