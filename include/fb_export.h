/*
 * fb_export.h
 *    Flashback mutation and export entry points.
 */

#ifndef FB_EXPORT_H
#define FB_EXPORT_H

#include "fb_common.h"

Datum pg_flashback_to(PG_FUNCTION_ARGS);
Datum pg_flashback_rewind(PG_FUNCTION_ARGS);

#endif
