# Arrow Relational Algebra

## broken-expr production projection

The hash-join key/payload projection runs through the generic Rust expression
runtime in [broken-expr-rs](https://github.com/zanmato1984/broken-expr-rs).
Native Arrow Datums and the query MemoryPool stay in C++; the synchronous bridge
passes checked column handles, with no serialization or duplicate buffer charge.
`Options::expression_scratch_limit` controls Rust batch-entry admission.

Build dependencies on macOS:

```bash
brew install cmake ninja folly googletest rapidjson gflags xsimd
export ARA_EXPR_BUILD_DIR="$HOME/dev/agent-work/openclaw/ara-expression-build"
# Optional local core checkout override, otherwise the pinned core SHA is fetched:
export ARA_BROKEN_EXPR_SOURCE_DIR="$HOME/dev/broken-expr-rs"
scripts/verify-broken-expr.sh
```

The script builds the actual ara library with its pinned Arrow 23, compares the
production projection with the original algorithm, runs the original join-build
and all 18 join-probe tests, disables the Rust kernel and confirms both production
paths fail, then restores the connected build and reruns the tests. Logs and an
actual Arrow function registry inventory are saved in the supplied build output.
The disconnected build is a test-only negative control, never a production flavor.
The integration does not migrate SQL builtin libraries or ara's entire executor.
Tracking: [ara #15](https://github.com/zanmato1984/ara/issues/15),
[core #1](https://github.com/zanmato1984/broken-expr-rs/issues/1).
