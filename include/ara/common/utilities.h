#pragma once

#include "ara/common/errors.h"
#include "ara/common/types.h"

namespace ara {

template <typename ID> inline SourceId makeHeapSourceId(ID id) {
  ARA_ASSERT(id > 0,
             "Making heap source ID from a non-positive ID is not allowed");
  return -static_cast<SourceId>(id);
}

inline bool isHeapSourceId(SourceId source_id) { return source_id < 0; }

} // namespace ara
