#ifndef FB_ENTRY_H
#define FB_ENTRY_H

#include "fb_common.h"

Datum fb_version(PG_FUNCTION_ARGS);
Datum fb_check_relation(PG_FUNCTION_ARGS);
Datum pg_flashback(PG_FUNCTION_ARGS);
Datum fb_export_undo(PG_FUNCTION_ARGS);

#endif
