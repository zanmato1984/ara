#pragma once

#include <arrow/api.h>
#include <arrow/util/logging.h>

#define ARA_CHECK ARROW_CHECK
#define ARA_CHECK_OK ARROW_CHECK_OK

// TODO: Check every DCHECK usage to see if it should be CHECK.
#define ARA_DCHECK ARROW_DCHECK
#define ARA_DCHECK_OK ARROW_DCHECK_OK

#define ARA_RETURN_NOT_OK ARROW_RETURN_NOT_OK
#define ARA_RETURN_IF ARROW_RETURN_IF
#define ARA_ASSIGN_OR_RAISE ARROW_ASSIGN_OR_RAISE

#define ARA_LOG ARROW_LOG
